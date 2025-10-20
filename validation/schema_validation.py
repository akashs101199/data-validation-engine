import pandera as pa
from pandera import Column, DataFrameSchema, Check

# Custom check to ensure min transaction value is not greater than max
def check_min_less_than_max(df):
    return df["Min_Transaction_Value"] <= df["Max_Transaction_Value"]

schema = DataFrameSchema({
    "Customer_ID": Column(
        pa.String,
        # CORRECTED LINE: Made regex more permissive to allow more data to pass to business rules
        Check.str_matches(r'^[a-zA-Z0-9_-]+$'),
        nullable=False
    ),
    "Age": Column(pa.Int, checks=[Check.ge(18), Check.le(100)]),
    "Location": Column(pa.String, Check.isin(["Urban", "Suburban", "Rural"])),
    "Income_Level": Column(pa.String, Check.isin(["Low", "Middle", "High"])),
    "Total_Transactions": Column(pa.Int, Check.ge(0)),
    "Avg_Transaction_Value": Column(pa.Float, Check.ge(1)),
    "Max_Transaction_Value": Column(pa.Float, Check.ge(0)),
    "Min_Transaction_Value": Column(pa.Float, Check.ge(0)),
    "Total_Spent": Column(pa.Float, Check.ge(0)),
    "Active_Days": Column(pa.Int, Check.ge(0)),
    "Last_Transaction_Days_Ago": Column(pa.Int, Check.ge(0)),
    "Loyalty_Points_Earned": Column(pa.Int, Check.ge(0)),
    "Referral_Count": Column(pa.Int, Check.ge(0)),
    "Cashback_Received": Column(pa.Float, Check.ge(0)),
    "App_Usage_Frequency": Column(pa.String, Check.isin(["Monthly", "Weekly", "Daily"])),
    "Preferred_Payment_Method": Column(pa.String),
    "Support_Tickets_Raised": Column(pa.Int, Check.ge(0)),
    "Issue_Resolution_Time": Column(pa.Float, Check.ge(0)),
    "Customer_Satisfaction_Score": Column(pa.Int, checks=[Check.ge(0), Check.le(10)]),
    "LTV": Column(pa.Float, Check.ge(0)),
},
# Add DataFrame-level checks for cross-column validation
checks=Check(check_min_less_than_max, element_wise=False, error="Min_Transaction_Value cannot be greater than Max_Transaction_Value")
)

def validate_schema(df, lazy=False):
    return schema.validate(df, lazy=lazy)