set -e
cp ../pipeline/setup_seenthis.py ../pipeline/setup.py

BRANCH=$(git symbolic-ref --short -q HEAD)

for NAME in ssp_keyword_v1 ssp_product_pagetype_environment_v1
do
	if [ "$BRANCH" == 'development' ]; then
    python read_dal_gcs_to_bq.py --project greenfield_dev \
      --runner DataflowRunner \
      --temp_location  gs://bbyus-integrate-rds-myads-d01-dataflow/temp/${NAME} \
      --template_location gs://bbyus-integrate-rds-myads-d01-dataflow/templates/${NAME} \
      --table_name ${NAME} \
      --dataset_id criteo \
      --file_format 'json' \
      --setup_file ../pipeline/setup.py

  elif [ "$BRANCH" == 'master' ]; then
    python criteo_bulk_dump_d01.py --project greenfield_prod \
      --runner DataflowRunner \
      --temp_location  gs://bbyus-integrate-rds-myads-p01-dataflow/temp/${NAME} \
      --template_location gs://bbyus-integrate-rds-myads-p01-dataflow/templates/${NAME} \
      --table_name ${NAME} \
      --dataset_id criteo \
      --file_format'json' \
      --setup_file ../pipeline/setup.py
  else
    echo "you are on branch \"$BRANCH\"; you must be on development or master to upload template"
    exit 0;
  fi
done