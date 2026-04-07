import pandas as pd
from faker import Faker
import numpy as np
import os
import random

fake = Faker('en_IN')

def generate_all_bronze(records=1000):
    os.makedirs('data/bronze', exist_ok=True)
    
    # Complete 40-Table Map for 121 Columns
    sources = {
        # LOS / CRM / KYC
        "src_crm_core.csv": ["cust_id", "pan_hash", "aadhaar_mask", "age_at_app"],
        "src_crm_profile.csv": ["cust_id", "highest_qual", "religion", "caste_category", "is_ews_flag"],
        "src_crm_geo.csv": ["cust_id", "cust_lat_long", "residence_pincode", "geo_cluster", "segment"],
        "src_mkt_leads.csv": ["app_id", "utm_source", "utm_medium", "campaign_id", "acquisition_cost"],
        "src_kyc_api.csv": ["cust_id", "kyc_status", "kyc_method"],
        "src_bureau.csv": ["cust_id", "cibil_score"],
        "src_aa_bank.csv": ["cust_id", "avg_monthly_bal", "salary_credit_dt", "chq_bounce_cnt", "monthly_spend_ratio"],
        "src_los_main.csv": ["app_id", "cust_id", "app_stage", "loan_amt_requested", "app_start_date"],
        "src_org_hierarchy.csv": ["branch_code", "branch_name", "bank_zone"],
        "src_org_staff.csv": ["rm_id", "rm_name", "sm_id", "sm_name", "bm_name", "region_mgr", "zonal_mgr", "cm_id", "cm_name"],
        "src_los_engine.csv": ["app_id", "decision_engine_v", "rule_triggered", "pd_score", "decision_code"],
        "src_los_uw.csv": ["app_id", "approved_limit", "uw_policy_id"],
        "src_product.csv": ["prod_id", "prod_type", "interest_rate_apr", "processing_fee_logic", "tenure_months"],
        "src_lms_disb.csv": ["disb_id", "loan_id", "disbursed_amt", "processing_fee", "bank_ref_no", "payout_timestamp"],
        "src_casa.csv": ["acc_id", "cust_id", "current_bal"],
        "src_rd.csv": ["rd_id", "cust_id", "maturity_amt", "installment_amt", "total_paid", "maturity_date"],
        "src_joint_acc.csv": ["mapping_id", "primary_cust_id", "secondary_cust_id", "relation_type", "joint_hold_flag"],
        "src_lms_master.csv": ["loan_id", "cust_id", "sanctioned_amt", "principal_os", "interest_os", "emi_amt_fixed", "loan_status"],
        "src_lms_emi.csv": ["emi_id", "loan_id", "due_date", "due_amt"],
        "src_lms_ledger.csv": ["txn_id", "loan_id", "amt_paid", "txn_date", "txn_type", "principal_comp", "interest_comp"],
        "src_risk_dpd.csv": ["loan_id", "current_dpd"],
        "src_risk_bounce.csv": ["loan_id", "emi_id", "bounce_reason"],
        "src_coll_alloc.csv": ["loan_id", "agent_id", "allocation_date"],
        "src_coll_call.csv": ["loan_id", "call_disposition", "ptp_date", "ptp_amt"],
        "src_coll_perf.csv": ["agent_id", "total_calls", "resolution_rate", "coll_efficiency_perc"],
        "src_ops_restruct.csv": ["loan_id", "event_type", "moratorium_flag", "old_roi", "new_roi", "approval_authority"],
        "src_ops_collateral.csv": ["loan_id", "asset_type", "valuation_amt", "ltv_ratio", "insurance_expiry"],
        "src_ops_grievance.csv": ["complaint_id", "cust_id", "category", "resolution_status", "ombudsman_flag", "ombudsman_ref"],
        "src_ops_audit.csv": ["field_changed", "old_val", "new_val"],
        "src_lms_closure.csv": ["loan_id", "closure_type", "noc_issued_dt"],
        "src_los_tat.csv": ["app_id", "doc_submission_date", "login_date", "sanction_date", "pf_date"],
        "src_los_rejects.csv": ["app_id", "lost_date", "lost_reason_code"],
        "src_los_metrics.csv": ["app_id", "tat_start_to_login", "tat_login_to_sanc"],
        # The Final 7
        "src_fin_gl.csv": ["txn_id", "dr_acc", "cr_acc", "amt_paid", "txn_date"],
        "src_fin_tax.csv": ["loan_id", "gst_paid", "tds_ded", "stamp_duty", "psl_eligible"],
        "src_ops_fi.csv": ["app_id", "fi_status", "lat_long_match", "residence_verified"],
        "src_hrms_staff.csv": ["user_id", "staff_name", "designation", "is_active"],
        "src_api_bsa.csv": ["cust_id", "salary_confidence", "avg_monthly_bal", "chq_bounce_cnt"],
        "src_sys_logs.csv": ["event_id", "field_changed", "old_val", "new_val", "timestamp"],
        "src_dms_status.csv": ["app_id", "doc_submission_date", "is_ocr_verified", "pending_docs"]
    }

    for filename, cols in sources.items():
        data = []
        for _ in range(records):
            row = {col: fake.word() for col in cols}
            # Add specific data generation logic here for IDs and Dates
            if "cust_id" in row: row["cust_id"] = random.randint(1000, 9999)
            if "loan_id" in row: row["loan_id"] = random.randint(10000, 99999)
            if "app_id" in row: row["app_id"] = random.randint(100000, 999999)
            data.append(row)
        
        pd.DataFrame(data).to_csv(f'data/bronze/{filename}', index=False)
    
    print(f"✅ Created {len(sources)} Bronze tables in data/bronze/")

if __name__ == "__main__":
    generate_all_bronze()
