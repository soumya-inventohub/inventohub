#!/usr/bin/env python3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import time

# ───────────────── Configuration ─────────────────
INPUT_PARQUET_S3_URL = 's3://epo.inventohub/merged/epo_2005_2024.parquet'
OUTPUT_PARQUET_S3_URL = 's3://epo.inventohub/merged/epo_2022_2024_data.parquet'

# ───────────────── Main Processing Logic ─────────────────
def main():
    s3 = s3fs.S3FileSystem()
    print(f"Opening Parquet file from S3 for chunked reading: {INPUT_PARQUET_S3_URL}...")
    
    writer = None # Initialize the Parquet writer as None
    total_rows_written = 0
    
    try:
        parquet_file = pq.ParquetFile(INPUT_PARQUET_S3_URL, filesystem=s3)
        print(f"Found {parquet_file.num_row_groups} chunks. Starting iteration...")

        for i in range(parquet_file.num_row_groups):
            start_chunk_time = time.time()
            df_chunk = parquet_file.read_row_group(i).to_pandas()
            df_filtered = df_chunk[
                (df_chunk['date_publication'] >= '20220101') & 
                (df_chunk['date_publication'] <= '20241231')
            ]
            
            chunk_time = time.time() - start_chunk_time

            if not df_filtered.empty:
                rows_in_chunk = len(df_filtered)
                total_rows_written += rows_in_chunk
                
                # Convert the pandas DataFrame to a pyarrow Table
                table = pa.Table.from_pandas(df_filtered, preserve_index=False)

                # ***** NEW INCREMENTAL WRITING LOGIC *****
                if writer is None:
                    # If this is the first chunk with data, open the file for writing
                    print(f"--> Chunk {i+1}: Found {rows_in_chunk} rows. Opening output file for writing... (Took {chunk_time:.2f}s)")
                    writer = pq.ParquetWriter(OUTPUT_PARQUET_S3_URL, table.schema, filesystem=s3)
                else:
                     print(f"--> Chunk {i+1}: Found {rows_in_chunk} rows. Appending to output file... (Took {chunk_time:.2f}s)")

                # Append the current chunk's data to the Parquet file on S3
                writer.write_table(table)
            else:
                print(f"--> Chunk {i+1}: Found 0 rows. (Took {chunk_time:.2f}s).")

        if total_rows_written == 0:
            print("⚠️ No data found for the years 2022-2024. No file was created.")
            return

    except Exception as e:
        print(f"❌ An error occurred during processing: {e}")
    finally:
        # This block ensures the writer is closed properly, finalizing the file
        if writer:
            print("\nAll chunks processed. Closing the final Parquet file...")
            writer.close()
            print("\n✅ Process complete! New Parquet file saved successfully to S3.")
            print(f"   Final file location: {OUTPUT_PARQUET_S3_URL}")
            print(f"   Total rows written: {total_rows_written}")

if __name__ == "__main__":
    main()