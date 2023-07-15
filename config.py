URL_GET_STACKS = "https://www.moderndatastack.xyz/stacks"

URL_STACK_BASE = "https://www.moderndatastack.xyz"

DIV_CLASS_FOR_STACKS = "justify-content-md-center"

source_path_mds = "data/source/mds"
stage_path_company = "data/staging/mds/company"
stage_path_stack = "data/staging/mds/stack"

company_cols = ['_id',
                  'companyName',
                  'description',
                  'organizationId',
                  'verified']

gcp_project_id = "modern-datastack-analysis"
bucket_name_source_mds = "datalake--sources"
bucket_source_dir_mds = "mds/"
bucket_source_dir_mds_combined = "mds/combined/"