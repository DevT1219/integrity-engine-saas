import pandas as pd
import yaml
import os

def run_master_parser():
    # 1. Load your 121-column Mapping instructions
    with open('configs/mapper_v1.yaml', 'r') as f:
        config = yaml.safe_load(f)

    os.makedirs('data/silver', exist_ok=True)

    # 2. Iterate through every model defined in your YAML
    for model in config['models']:
        model_name = model['name']
        sources = model['sources']
        join_key = model.get('join_on', None)
        target_cols = model['columns']

        print(f"🔄 Processing: {model_name}...")

        try:
            # Start with the first source table
            base_df = pd.read_csv(f'data/bronze/{sources[0]}.csv')

            # Join additional source tables if they exist
            for extra_source in sources[1:]:
                extra_df = pd.read_csv(f'data/bronze/{extra_source}.csv')
                if join_key:
                    base_df = base_df.merge(extra_df, on=join_key, how='left')

            # Only keep the columns you defined in the YAML
            # We use set intersection to avoid errors if a column is missing
            available_cols = [c for c in target_cols if c in base_df.columns]
            final_df = base_df[available_cols]

            # Save the clean Silver Model
            final_df.to_csv(f'data/silver/{model_name}.csv', index=False)
            print(f"✅ Created {model_name} with {len(available_cols)} columns.")

        except Exception as e:
            print(f"❌ Error building {model_name}: {e}")

if __name__ == "__main__":
    run_master_parser()