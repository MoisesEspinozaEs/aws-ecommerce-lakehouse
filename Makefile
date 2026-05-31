# Convenience targets for the demo. On Windows without `make`, run the underlying
# commands shown in the README instead.

LAKE_BUCKET ?= $(shell cd infra && terraform output -raw lake_bucket)
REGION ?= us-east-1

deploy:        ## Stand up the infrastructure
	cd infra && terraform init && terraform apply

seed:          ## Upload the product catalog
	python scripts/seed_products.py --bucket $(LAKE_BUCKET) --region $(REGION)

events:        ## Send sample clickstream to Kinesis
	python scripts/send_sample_events.py --count 5000 --region $(REGION)

silver:        ## Run the Glue job (bronze -> silver)
	aws glue start-job-run --job-name ecomlake-dev-bronze-to-silver --region $(REGION)

gold:          ## Build gold and run quality tests
	cd dbt && dbt deps && LAKE_BUCKET=$(LAKE_BUCKET) dbt build --profiles-dir .

destroy:       ## Tear everything down (~$0)
	cd infra && terraform destroy

.PHONY: deploy seed events silver gold destroy
