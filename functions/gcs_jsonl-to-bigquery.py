import dlt
from dlt.sources.filesystem import filesystem, read_jsonl


def pipeline_run(bucket_name:str, prefix_filter:list[str]):
    # TODO: figure out how to handle merging multiple extracts and deduping data probably a feature within dlt that I still have to learn
    files = filesystem(bucket_url=f"gs://{bucket_name}", file_glob=f"{prefix_filter}*.jsonl")
    # NOTE: probably better to filter the files to remove some files that are not as important
    # filtered_files = [f for f in files if f['file_name'] in ['Marquee.jsonl', "Playlist1.jsonl", "SearchQueries.jsonl", "Userdata.jsonl", "YourLibrary.jsonl"]]
    reader = (files | read_jsonl()).with_name("spotify_account_data")
    pipeline = dlt.pipeline(pipeline_name="spotify_account_data_pipeline", dataset_name="spotify_account_data", destination="bigquery")

    # NOTE: learn how dlt manages the auto schema from jsonl... the output tables are quite gross imo but it could also be user error
    info = pipeline.run(reader)
    print(info)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transfers data from a .jsonl file on GCS to BigQuery using dlt")
    parser.add_argument("--bucket_name", type=str, required=True, help="name of the bucket where the .jsonl files are stored")
    parser.add_argument("--prefix_filter", type=str, required=True, help="prefix path location where the .jsonl files are stored")
    args = parser.parse_args()
    
    pipeline_run(args.bucket_name, args.prefix_filter)