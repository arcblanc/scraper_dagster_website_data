"""Shared arithmetic for the credit-desk notebooks.

The functions read the CSV extracts under data/ and return frames a
counterparty desk can rank. Identity fields on the SSM profile payload
(identity number, date of birth, street address, person name) are never
copied into those frames.
"""

from __future__ import annotations

import ast
import math
from pathlib import Path

import numpy as np
import pandas as pd

# notebooks/credit_desk/credit_stats.py -> repository root is parents[2].
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPOSITORY_ROOT / "data"
FIGURE_DIR = Path(__file__).resolve().parent / "figures"

# Altman Z″ cut-offs for the four-ratio emerging-market score
# (the form without the 3.25 location constant).
# Z″ > 2.6 safe zone, 1.1 ≤ Z″ ≤ 2.6 grey zone, Z″ < 1.1 distress zone.
ALTMAN_SAFE_CUTOFF = 2.6
ALTMAN_DISTRESS_CUTOFF = 1.1

# A modified z-score past this absolute value is a MAD outlier.
# 0.6745 scales MAD so it matches the standard deviation on a normal sample.
MODIFIED_Z_CUTOFF = 3.5
MAD_NORMAL_CONSISTENCY = 0.6745

# Bursa statement lines are in RM millions (Maybank FY2024 revenue is 27,907).
LISTED_AMOUNT_UNIT = "RM millions"

# SSM financial lines are in ringgit (a sample revenue is 26,577,900).
SSM_AMOUNT_UNIT = "RM"

FINANCIAL_SECTOR_NAME = "Financials"

# Listed-company ownership buckets on public_complete_ownership_type.
INSTITUTIONAL_INVESTOR_TYPES = {
    "Investment Advisor",
    "Mutual Fund",
    "Investment Advisor/Hedge Fund",
    "Sovereign Wealth Fund",
    "Pension Fund",
    "Bank and Trust",
    "Government Agency",
    "Insurance Company",
    "Pension Fund Portfolio",
    "Venture Capital",
    "Hedge Fund",
    "Hedge Fund Portfolio",
    "Private Equity",
    "Endowment Fund",
}

# When the same SSM company-year arrives from more than one vendor,
# keep the earlier rank. SSM_CBID is the commission extract.
SSM_SOURCE_RANK = {
    "SSM_CBID": 0,
    "CRIF_OMESTI": 1,
    "RAFFTECH": 2,
    "INFOMINA": 3,
}

# SSM statusOfCompany letters seen on this extract.
SSM_STATUS_LABEL = {
    "E": "Existing",
    "W": "Winding up",
    "D": "Dissolved",
}

STATE_ALIASES = {
    "PENANG": "PULAU PINANG",
    "PULAU PINANG": "PULAU PINANG",
    "MALACCA": "MELAKA",
    "MELAKA": "MELAKA",
    "KUALA LUMPUR": "WILAYAH PERSEKUTUAN",
    "WILAYAH PERSEKUTUAN": "WILAYAH PERSEKUTUAN",
    "WP KUALA LUMPUR": "WILAYAH PERSEKUTUAN",
    "W.P. KUALA LUMPUR": "WILAYAH PERSEKUTUAN",
    "W P KUALA LUMPUR": "WILAYAH PERSEKUTUAN",
    "WILAYAH PERSEKUTUAN KUALA LUMPUR": "WILAYAH PERSEKUTUAN",
    "W.P. PUTRAJAYA": "PUTRAJAYA",
    "W P PUTRAJAYA": "PUTRAJAYA",
    "PUTRAJAYA": "PUTRAJAYA",
    "W.P. LABUAN": "LABUAN",
    "W P LABUAN": "LABUAN",
    "LABUAN": "LABUAN",
    "KOTA KINABALU": "SABAH",
    "KUCHING": "SARAWAK",
    "MIRI": "SARAWAK",
}


def parse_number(value) -> float:
    """Turn a statement cell into a float.

    Cells arrive as 149.87, as '10,506.39', or as '56.07%'.
    A blank, a dash, or a non-numeric token becomes NaN.
    Parentheses mean a negative amount, the usual statement convention.
    """
    if value is None:
        return math.nan
    if isinstance(value, (int, float, np.integer, np.floating)):
        number = float(value)
        if math.isnan(number):
            return math.nan
        return number

    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "-", "na", "n/a"}:
        return math.nan

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace(",", "").replace("%", "").replace("RM", "").replace("rm", "")
    text = text.strip()
    try:
        number = float(text)
    except ValueError:
        return math.nan
    if negative:
        number = -number
    return number


def parse_number_series(values: pd.Series) -> pd.Series:
    """Apply parse_number to a column and keep the original index."""
    return values.map(parse_number).astype(float)


def ownership_fraction(value) -> float:
    """Turn '56.07%' or 56.07 into a fraction in [0, 1] scale.

    The Bursa extracts store stakes as percent strings. A value above 1.5
    is percent points even when the percent sign was already stripped.
    A fractional 0.56 without a percent sign is left as a fraction.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return math.nan
    raw_text = str(value)
    number = parse_number(value)
    if math.isnan(number):
        return math.nan
    if "%" in raw_text or abs(number) > 1.5:
        return number / 100.0
    return number


def wilson_interval(success_count: int, trial_count: int, z_value: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a share.

    The Wilson interval stays inside [0, 1] when the share is near 0 or 1,
    which is where a normal approximation for a distress rate breaks.
    Returns (nan, nan) when there are no trials.
    """
    if trial_count <= 0:
        return (math.nan, math.nan)
    sample_share = success_count / trial_count
    z_squared = z_value ** 2
    denominator = 1.0 + z_squared / trial_count
    centre = (sample_share + z_squared / (2.0 * trial_count)) / denominator
    margin = (
        z_value
        * math.sqrt(
            (sample_share * (1.0 - sample_share) + z_squared / (4.0 * trial_count))
            / trial_count
        )
        / denominator
    )
    return (max(0.0, centre - margin), min(1.0, centre + margin))


def modified_z_scores(values: pd.Series) -> pd.Series:
    """Robust z-score using the median and the median absolute deviation.

    Financial ratios have a long right tail, so a mean-and-sd z-score
    flags the tail and then moves the mean. MAD does not.
    The 0.6745 factor makes a normal sample line up with the usual z-score.
    """
    numeric = pd.to_numeric(values, errors="coerce")
    median_value = numeric.median()
    absolute_deviation = (numeric - median_value).abs()
    mad = absolute_deviation.median()
    if mad == 0 or math.isnan(mad):
        return pd.Series(np.nan, index=values.index)
    return MAD_NORMAL_CONSISTENCY * (numeric - median_value) / mad


def distribution_row(values: pd.Series, ratio_name: str) -> dict:
    """Median, IQR, skew, and the share of MAD outliers for one ratio.

    Mean is reported beside the median because the gap between them is
    the skew a credit limit should not be set on.
    """
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    observation_count = int(numeric.shape[0])
    if observation_count == 0:
        return {
            "ratio": ratio_name,
            "n": 0,
            "median": math.nan,
            "mean": math.nan,
            "p25": math.nan,
            "p75": math.nan,
            "iqr": math.nan,
            "skew": math.nan,
            "excess_kurtosis": math.nan,
            "mad_outlier_share": math.nan,
        }

    from scipy.stats import kurtosis, skew

    quartile_25 = float(numeric.quantile(0.25))
    quartile_75 = float(numeric.quantile(0.75))
    z_scores = modified_z_scores(numeric).abs()
    return {
        "ratio": ratio_name,
        "n": observation_count,
        "median": float(numeric.median()),
        "mean": float(numeric.mean()),
        "p25": quartile_25,
        "p75": quartile_75,
        "iqr": quartile_75 - quartile_25,
        "skew": float(skew(numeric.to_numpy(), bias=False)),
        "excess_kurtosis": float(kurtosis(numeric.to_numpy(), bias=False, fisher=True)),
        "mad_outlier_share": float((z_scores > MODIFIED_Z_CUTOFF).mean()),
    }


def altman_z_double_prime(
    working_capital: pd.Series,
    retained_earnings: pd.Series,
    operating_income: pd.Series,
    book_equity: pd.Series,
    total_assets: pd.Series,
    total_liabilities: pd.Series,
) -> pd.Series:
    """Emerging-market Altman Z″ from book items.

    Z″ = 6.56 X₁ + 3.26 X₂ + 6.72 X₃ + 1.05 X₄
    X₁ = working capital / total assets
    X₂ = retained earnings / total assets
    X₃ = operating income / total assets (EBIT proxy)
    X₄ = book equity / total liabilities

    Sales / assets is left out on purpose: it moves with the sector, and
    this file mixes retailers with utilities. A non-positive asset or
    liability base makes the ratio undefined.
    """
    assets = total_assets.where(total_assets > 0)
    liabilities = total_liabilities.where(total_liabilities > 0)
    working_capital_ratio = working_capital / assets
    retained_earnings_ratio = retained_earnings / assets
    operating_return = operating_income / assets
    equity_to_liabilities = book_equity / liabilities
    return (
        6.56 * working_capital_ratio
        + 3.26 * retained_earnings_ratio
        + 6.72 * operating_return
        + 1.05 * equity_to_liabilities
    )


def distress_zone(z_double_prime: pd.Series) -> pd.Series:
    """Map Z″ onto safe / grey / distress using the 2.6 and 1.1 cut-offs."""
    zones = pd.Series(pd.NA, index=z_double_prime.index, dtype="object")
    known = z_double_prime.notna()
    zones.loc[known & (z_double_prime > ALTMAN_SAFE_CUTOFF)] = "safe"
    zones.loc[
        known
        & (z_double_prime >= ALTMAN_DISTRESS_CUTOFF)
        & (z_double_prime <= ALTMAN_SAFE_CUTOFF)
    ] = "grey"
    zones.loc[known & (z_double_prime < ALTMAN_DISTRESS_CUTOFF)] = "distress"
    return zones


def herfindahl_from_positive_amounts(amounts: list[float]) -> float:
    """HHI = Σ (shareᵢ)² on a list of positive amounts that form one whole.

    Equal stakes on two holders give 0.5² + 0.5² = 0.5.
    One holder gives 1. An empty list is undefined.
    """
    positive_amounts = [amount for amount in amounts if amount is not None and amount > 0 and not math.isnan(amount)]
    total_amount = sum(positive_amounts)
    if total_amount <= 0 or not positive_amounts:
        return math.nan
    squared_share_total = 0.0
    for amount in positive_amounts:
        share = amount / total_amount
        squared_share_total += share * share
    return squared_share_total


def largest_share(amounts: list[float]) -> float:
    """Largest holder's fraction of the same whole used for the HHI."""
    positive_amounts = [amount for amount in amounts if amount is not None and amount > 0 and not math.isnan(amount)]
    total_amount = sum(positive_amounts)
    if total_amount <= 0 or not positive_amounts:
        return math.nan
    return max(positive_amounts) / total_amount


def cramers_v(chi_square_statistic: float, observation_count: int, row_count: int, column_count: int) -> float:
    """Effect size for a chi-square table.

    V = sqrt( χ² / (n × (min(rows, columns) − 1)) ).
    On a million-row registry every p-value collapses, so V is the
    quantity that says whether the state mix actually differs.
    """
    degrees_base = min(row_count, column_count) - 1
    if observation_count <= 0 or degrees_base <= 0:
        return math.nan
    return math.sqrt(chi_square_statistic / (observation_count * degrees_base))


def epsilon_squared(h_statistic: float, group_count: int, observation_count: int) -> float:
    """Effect size for a Kruskal-Wallis H.

    ε² = (H − k + 1) / (n − k). Zero means the sector medians do not
    separate beyond what the ranks already allow. Negative values are
    clipped to zero when H is smaller than the degrees of freedom.
    """
    if observation_count <= group_count:
        return math.nan
    effect = (h_statistic - group_count + 1) / (observation_count - group_count)
    return max(0.0, effect)


def age_in_years(dates: pd.Series, as_of: pd.Timestamp | None = None) -> pd.Series:
    """Whole-year age from a date column to today.

    `utc=True` accepts a mix of naive timestamps and any zone, then the
    zone is dropped so the subtraction is a plain number of days.
    """
    parsed = pd.to_datetime(dates, errors="coerce", utc=True).dt.tz_localize(None)
    if as_of is None:
        as_of = pd.Timestamp.utcnow().tz_localize(None).normalize()
    return (as_of - parsed).dt.days / 365.25


def latest_rows(frame: pd.DataFrame, company_column: str, date_column: str) -> pd.DataFrame:
    """One row per company: the latest non-null date."""
    ordered = frame.dropna(subset=[date_column]).sort_values(
        [company_column, date_column], ascending=[True, False]
    )
    return ordered.drop_duplicates(company_column, keep="first").reset_index(drop=True)


def clean_state_name(raw_state) -> str:
    """Fold spelling variants and mark a dash or a blank as missing.

    A dash is how the registry extract records an unknown state. It is
    not a state, so it stays out of the geographic chi-square.
    """
    if raw_state is None or (isinstance(raw_state, float) and math.isnan(raw_state)):
        return "MISSING"
    text = str(raw_state).strip().upper()
    text = " ".join(text.replace(".", " ").split())
    if text in {"", "-", "NAN", "NONE", "NA", "(NA)"}:
        return "MISSING"
    return STATE_ALIASES.get(text, text)


def _read_csv(file_name: str) -> pd.DataFrame:
    path = DATA_DIR / file_name
    if not path.exists():
        raise FileNotFoundError(f"Expected extract at {path}")
    return pd.read_csv(path, low_memory=False)


def load_listed_statements() -> dict[str, pd.DataFrame]:
    """Balance sheet, income, profile, and cash-flow lines, with numbers parsed.

    Join key for a statement year is registration_number + fiscal_date.
    Amounts stay in RM millions.
    """
    balance_sheet = _read_csv("balance_sheet.csv")
    income_statement = _read_csv("income.csv")
    company_profile = _read_csv("company_profile.csv")
    cash_flow = _read_csv("cash_flow.csv")

    for frame in (balance_sheet, income_statement, company_profile, cash_flow):
        frame["registration_number"] = frame["registration_number"].astype(str).str.strip()

    balance_amount_columns = [
        "retained_earnings_accumulated_deficit",
        "total_equity",
        "total_debt",
        "total_current_assets",
        "total_assets",
        "total_current_liabilities",
        "total_liabilities",
    ]
    income_amount_columns = [
        "revenue",
        "net_income_before_taxes",
        "net_income_after_taxes",
        "operating_income",
        "gross_profit",
        "cost_of_revenue",
        "basic_eps_including_extraordinary_items",
    ]
    cash_flow_amount_columns = [
        "net_income_starting_line",
        "depreciation_depletion",
        "depreciation",
    ]

    for column in balance_amount_columns:
        balance_sheet[column] = parse_number_series(balance_sheet[column])
    for column in income_amount_columns:
        income_statement[column] = parse_number_series(income_statement[column])
    for column in cash_flow_amount_columns:
        cash_flow[column] = parse_number_series(cash_flow[column])

    balance_sheet["fiscal_date"] = pd.to_datetime(
        balance_sheet["fiscal_date"], format="%d %b %Y", errors="coerce"
    )
    income_statement["fiscal_date"] = pd.to_datetime(
        income_statement["fiscal_date"], format="%d %b %Y", errors="coerce"
    )
    cash_flow["fiscal_date"] = pd.to_datetime(
        cash_flow["fiscal_date"], format="%d %b %Y", errors="coerce"
    )

    company_profile["sector"] = company_profile["sector"].fillna("Unknown").astype(str).str.strip()
    company_profile["market_cap_mil"] = parse_number_series(company_profile["market_cap_mil"])

    return {
        "balance_sheet": balance_sheet,
        "income_statement": income_statement,
        "company_profile": company_profile,
        "cash_flow": cash_flow,
    }


def build_listed_panel(statements: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per company per fiscal date, with credit ratios attached.

    Banks and other Financials stay in the frame and carry
    is_financial_sector = True. Ratio medians for an industrial credit
    file should filter them out: a deposit book makes the current ratio
    a different object.
    """
    balance_columns = [
        "registration_number",
        "fiscal_date",
        "retained_earnings_accumulated_deficit",
        "total_equity",
        "total_debt",
        "total_current_assets",
        "total_assets",
        "total_current_liabilities",
        "total_liabilities",
    ]
    income_columns = [
        "registration_number",
        "fiscal_date",
        "revenue",
        "net_income_before_taxes",
        "net_income_after_taxes",
        "operating_income",
        "gross_profit",
        "cost_of_revenue",
    ]
    profile_columns = [
        "registration_number",
        "company_name_bursa",
        "sector",
        "sub_sector",
        "market_cap_mil",
    ]

    balance_sheet = statements["balance_sheet"][balance_columns].drop_duplicates(
        ["registration_number", "fiscal_date"], keep="first"
    )
    income_statement = statements["income_statement"][income_columns].drop_duplicates(
        ["registration_number", "fiscal_date"], keep="first"
    )
    panel = balance_sheet.merge(
        income_statement,
        on=["registration_number", "fiscal_date"],
        how="inner",
    )
    panel = panel.merge(
        statements["company_profile"][profile_columns],
        on="registration_number",
        how="left",
    )
    panel["sector"] = panel["sector"].fillna("Unknown")
    panel["is_financial_sector"] = panel["sector"].eq(FINANCIAL_SECTOR_NAME)

    # Working capital is the current-asset cushion after current liabilities.
    panel["working_capital"] = panel["total_current_assets"] - panel["total_current_liabilities"]
    positive_assets = panel["total_assets"].where(panel["total_assets"] > 0)
    positive_liabilities = panel["total_liabilities"].where(panel["total_liabilities"] > 0)
    positive_equity = panel["total_equity"].where(panel["total_equity"] > 0)
    positive_revenue = panel["revenue"].where(panel["revenue"] > 0)
    positive_current_liabilities = panel["total_current_liabilities"].where(
        panel["total_current_liabilities"] > 0
    )

    panel["current_ratio"] = panel["total_current_assets"] / positive_current_liabilities
    panel["working_capital_to_assets"] = panel["working_capital"] / positive_assets
    panel["liabilities_to_assets"] = panel["total_liabilities"] / positive_assets
    panel["debt_to_equity"] = panel["total_debt"] / positive_equity
    panel["gross_margin"] = panel["gross_profit"] / positive_revenue
    panel["operating_margin"] = panel["operating_income"] / positive_revenue
    panel["net_profit_margin"] = panel["net_income_after_taxes"] / positive_revenue
    panel["return_on_assets"] = panel["net_income_after_taxes"] / positive_assets
    panel["return_on_equity"] = panel["net_income_after_taxes"] / positive_equity
    panel["z_double_prime"] = altman_z_double_prime(
        working_capital=panel["working_capital"],
        retained_earnings=panel["retained_earnings_accumulated_deficit"],
        operating_income=panel["operating_income"],
        book_equity=panel["total_equity"],
        total_assets=panel["total_assets"],
        total_liabilities=panel["total_liabilities"],
    )
    panel["distress_zone"] = distress_zone(panel["z_double_prime"])
    panel["is_loss_making"] = panel["net_income_after_taxes"] < 0
    return panel.sort_values(["registration_number", "fiscal_date"]).reset_index(drop=True)


def _literal_payload(raw_payload) -> dict | None:
    """Parse an SSM `data` cell that was stored as a Python dict string.

    JSON parsing fails because the extract uses single quotes. literal_eval
    accepts that form and does not execute calls.
    """
    if not isinstance(raw_payload, str) or not raw_payload.strip():
        return None
    try:
        payload = ast.literal_eval(raw_payload)
    except (SyntaxError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def parse_ssm_profiles(profile_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    """One safe row per SSM company.

    Officer and shareholder identity numbers, dates of birth, street
    addresses, and names are read only as far as literal_eval requires
    and are not written to any column. Counts, designation mix, share
    amounts, charge amounts, and the business-address state are kept.
    """
    if profile_frame is None:
        profile_frame = _read_csv("ssm_data/company_profiles.csv")

    profile_rows = []
    for registration_number, raw_payload, updated_at in zip(
        profile_frame["registration_no"].astype(str),
        profile_frame["data"],
        profile_frame["updated_at"],
        strict=True,
    ):
        payload = _literal_payload(raw_payload)
        if payload is None:
            continue

        basic_info = payload.get("basicInfo") or {}
        business_address = payload.get("businessAddress") or {}
        business_codes = payload.get("businessCodes") or []
        charges = payload.get("chargesInfo") or []
        officers = payload.get("officers") or []
        shareholders = payload.get("shareholders") or []
        share_capital = payload.get("shareCapital") or {}

        primary_code = ""
        primary_code_name = ""
        # priority 1 is the principal MSIC activity when the vendor filled it.
        ordered_codes = sorted(
            business_codes,
            key=lambda item: parse_number(item.get("priority"))
            if isinstance(item, dict)
            else math.inf,
        )
        for code_item in ordered_codes:
            if not isinstance(code_item, dict):
                continue
            code_name = str(code_item.get("businessCodeName") or "").strip()
            code_value = str(code_item.get("businessCode") or "").strip()
            if code_name or code_value:
                primary_code = code_value
                primary_code_name = code_name
                break

        director_count = 0
        secretary_count = 0
        for officer in officers:
            if not isinstance(officer, dict):
                continue
            designation = str(officer.get("designationCode") or "").upper()
            if "DIRECTOR" in designation:
                director_count += 1
            if "SECRETARY" in designation:
                secretary_count += 1

        share_amounts = []
        for holder in shareholders:
            if not isinstance(holder, dict):
                continue
            share_amount = parse_number(holder.get("share"))
            if math.isnan(share_amount):
                share_amount = parse_number(holder.get("shareVol"))
            if not math.isnan(share_amount) and share_amount > 0:
                share_amounts.append(share_amount)

        unsatisfied_charge_count = 0
        unsatisfied_charge_amount = 0.0
        for charge in charges:
            if not isinstance(charge, dict):
                continue
            charge_status = str(charge.get("chargeStatus") or "").strip().upper()
            if charge_status == "UNSATISFIED":
                unsatisfied_charge_count += 1
                charge_amount = parse_number(charge.get("chargeAmount"))
                if not math.isnan(charge_amount):
                    unsatisfied_charge_amount += charge_amount

        status_code = str(basic_info.get("statusOfCompany") or "").strip().upper()
        incorporation_text = basic_info.get("incorpDate")
        profile_rows.append(
            {
                "registration_number": registration_number.strip(),
                "company_status_code": status_code,
                "company_status": SSM_STATUS_LABEL.get(status_code, status_code or "Unknown"),
                "company_type": str(basic_info.get("companyType") or "").strip() or "Unknown",
                "incorporation_date": pd.to_datetime(incorporation_text, errors="coerce"),
                "primary_business_code": primary_code,
                "primary_business_name": primary_code_name,
                "business_state": clean_state_name(business_address.get("state")),
                "officer_count": len([item for item in officers if isinstance(item, dict)]),
                "director_count": director_count,
                "secretary_count": secretary_count,
                "shareholder_count": len(share_amounts),
                "shareholder_hhi": herfindahl_from_positive_amounts(share_amounts),
                "largest_shareholder_fraction": largest_share(share_amounts),
                "charge_count": len([item for item in charges if isinstance(item, dict)]),
                "unsatisfied_charge_count": unsatisfied_charge_count,
                "unsatisfied_charge_amount": unsatisfied_charge_amount,
                "issued_capital": parse_number(share_capital.get("totalIssued")),
                "updated_at": pd.to_datetime(updated_at, errors="coerce"),
            }
        )

    profiles = pd.DataFrame(profile_rows)
    if profiles.empty:
        return profiles
    # The extract repeats a registration number when a later vendor refresh
    # arrived. The credit file keeps the newest payload.
    profiles = profiles.sort_values("updated_at").drop_duplicates(
        "registration_number", keep="last"
    )
    return profiles.drop(columns=["updated_at"]).reset_index(drop=True)


def parse_ssm_financials(financial_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per company-year after vendor duplicates are collapsed.

    Profit before tax stands in for EBIT. Interest expense is not on this
    extract, so the proxy is lower than EBIT whenever interest was paid.
    """
    if financial_frame is None:
        financial_frame = _read_csv("ssm_data/company_financial_records.csv")

    yearly_rows = []
    for registration_number, fiscal_year, raw_payload, source, updated_at in zip(
        financial_frame["registration_no"].astype(str),
        financial_frame["year"],
        financial_frame["data"],
        financial_frame["source"],
        financial_frame["updated_at"],
        strict=True,
    ):
        payload = _literal_payload(raw_payload)
        if payload is None:
            continue

        current_assets = parse_number(payload.get("current_assets"))
        non_current_assets = parse_number(payload.get("non_current_assets"))
        if math.isnan(non_current_assets):
            non_current_assets = parse_number(payload.get("fixed_asset"))
        total_assets = current_assets + non_current_assets
        if math.isnan(current_assets) or math.isnan(non_current_assets):
            total_assets = math.nan

        total_liabilities = parse_number(payload.get("liability"))
        long_term_liabilities = parse_number(payload.get("long_term_liability"))
        current_liabilities = math.nan
        if not math.isnan(total_liabilities) and not math.isnan(long_term_liabilities):
            # liability on this payload behaves as total liabilities:
            # assets − liability is close to share capital + retained earnings.
            current_liabilities = total_liabilities - long_term_liabilities
            if current_liabilities < 0:
                current_liabilities = math.nan

        share_capital = parse_number(payload.get("share_capital"))
        retained_earning = parse_number(payload.get("retained_earning"))
        book_equity = math.nan
        if not math.isnan(share_capital) and not math.isnan(retained_earning):
            book_equity = share_capital + retained_earning

        revenue = parse_number(payload.get("revenue"))
        if math.isnan(revenue) or revenue == 0:
            revenue = parse_number(payload.get("total_revenue"))

        audit_flag = str(payload.get("unqualified_reports") or "").strip().upper()
        yearly_rows.append(
            {
                "registration_number": registration_number.strip(),
                "fiscal_year": int(fiscal_year),
                "source": str(source),
                "updated_at": pd.to_datetime(updated_at, errors="coerce"),
                "revenue": revenue,
                "profit_before_tax": parse_number(payload.get("profit_before_tax")),
                "profit_after_tax": parse_number(payload.get("profit_after_tax")),
                "current_assets": current_assets,
                "non_current_assets": non_current_assets,
                "total_assets": total_assets,
                "total_liabilities": total_liabilities,
                "long_term_liabilities": long_term_liabilities,
                "current_liabilities": current_liabilities,
                "book_equity": book_equity,
                "retained_earning": retained_earning,
                "unqualified_report": audit_flag == "Y",
                "audit_flag_present": audit_flag in {"Y", "N"},
                "qualified_or_other_report": audit_flag == "N",
            }
        )

    yearly = pd.DataFrame(yearly_rows)
    if yearly.empty:
        return yearly

    yearly["source_rank"] = yearly["source"].map(SSM_SOURCE_RANK).fillna(9)
    yearly = yearly.sort_values(
        ["registration_number", "fiscal_year", "source_rank", "updated_at"],
        ascending=[True, True, True, False],
    )
    yearly = yearly.drop_duplicates(
        ["registration_number", "fiscal_year"], keep="first"
    ).reset_index(drop=True)

    positive_assets = yearly["total_assets"].where(yearly["total_assets"] > 0)
    positive_revenue = yearly["revenue"].where(yearly["revenue"] > 0)
    positive_current_liabilities = yearly["current_liabilities"].where(yearly["current_liabilities"] > 0)
    positive_equity = yearly["book_equity"].where(yearly["book_equity"] > 0)

    yearly["current_ratio"] = yearly["current_assets"] / positive_current_liabilities
    yearly["liabilities_to_assets"] = yearly["total_liabilities"] / positive_assets
    yearly["net_profit_margin"] = yearly["profit_after_tax"] / positive_revenue
    yearly["return_on_assets"] = yearly["profit_after_tax"] / positive_assets
    yearly["return_on_equity"] = yearly["profit_after_tax"] / positive_equity
    yearly["working_capital"] = yearly["current_assets"] - yearly["current_liabilities"]
    yearly["z_double_prime"] = altman_z_double_prime(
        working_capital=yearly["working_capital"],
        retained_earnings=yearly["retained_earning"],
        operating_income=yearly["profit_before_tax"],
        book_equity=yearly["book_equity"],
        total_assets=yearly["total_assets"],
        total_liabilities=yearly["total_liabilities"],
    )
    yearly["distress_zone"] = distress_zone(yearly["z_double_prime"])
    yearly["is_loss_making"] = yearly["profit_after_tax"] < 0
    return yearly


def share_with_interval(success_mask: pd.Series) -> dict:
    """Point share plus a Wilson interval. NaN rows are not trials."""
    observed = success_mask.dropna()
    success_count = int(observed.sum())
    trial_count = int(observed.shape[0])
    low, high = wilson_interval(success_count, trial_count)
    share = success_count / trial_count if trial_count else math.nan
    return {
        "successes": success_count,
        "trials": trial_count,
        "share": share,
        "wilson_low": low,
        "wilson_high": high,
    }


def peer_percentile(values: pd.Series, group_labels: pd.Series) -> pd.Series:
    """Percentile rank of each company inside its own sector.

    0 is the weakest value in the sector, 1 is the strongest.
    A sector of one company is left as NaN: it has no peer.
    """
    frame = pd.DataFrame({"value": values, "group": group_labels})
    return frame.groupby("group")["value"].rank(method="average", pct=True).where(
        frame.groupby("group")["value"].transform("count") >= 5
    )
