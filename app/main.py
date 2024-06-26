import streamlit as st
import pandas as pd
import boto3
import os
from io import BytesIO
import requests

# Configure AWS S3
region_name = 'ap-south-1'
bucket_name = 'excel-integrator-inventory-bucket'

s3_client = boto3.client(
    's3',
    region_name=region_name,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

def generate_presigned_url(bucket_name, object_name, expiration=3600):
    response = s3_client.generate_presigned_url('put_object',
                                                Params={'Bucket': bucket_name, 'Key': object_name},
                                                ExpiresIn=expiration)
    return response

def upload_to_s3_via_presigned_url(url, data):
    headers = {'Content-Type': 'application/octet-stream'}
    response = requests.put(url, data=data, headers=headers)
    response.raise_for_status()

def generate_presigned_url_for_download(bucket_name, object_name, expiration=3600):
    response = s3_client.generate_presigned_url('get_object',
                                                Params={'Bucket': bucket_name, 'Key': object_name},
                                                ExpiresIn=expiration)
    return response

def read_excel_preview_from_s3(bucket_name, input_file_name):
    # Download file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=input_file_name)
    file = BytesIO(obj['Body'].read())

    # Read the Excel file
    xls = pd.ExcelFile(file)

    # Get the first sheet name
    first_sheet_name = xls.sheet_names[0]

    # Read the first sheet into a DataFrame
    first_sheet_df = xls.parse(first_sheet_name, header=None)

    return first_sheet_name, first_sheet_df

def load_and_process_excel(bucket_name, input_file_name, columns_to_fetch, header_row_index):
    # Download file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=input_file_name)
    file = BytesIO(obj['Body'].read())
    
    # Read the Excel file
    xls = pd.ExcelFile(file)
    
    all_data = []

    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name, header=None)
        
        # Ensure header_row_index is within bounds for the current sheet
        if header_row_index >= len(df):
            continue
        
        df.columns = df.iloc[header_row_index]
        df = df.drop(range(header_row_index + 1))
        
        # Reset the index of the DataFrame
        df.reset_index(drop=True, inplace=True)

        # Check if all selected columns exist in the sheet
        if not all(col in df.columns for col in columns_to_fetch):
            continue  # Skip this sheet if any of the selected columns are missing

        # Fill NaN values with 0
        df = df.fillna(0)

        # Ensure "Sr. No." column exists before processing
        if "Sr. No." in df.columns:
            # Convert "Sr. No." to numeric and filter out rows where it is not an integer
            df["Sr. No."] = pd.to_numeric(df["Sr. No."], errors='coerce')
            df = df.dropna(subset=["Sr. No."])
            df["Sr. No."] = df["Sr. No."].astype(int)
            df = df[df["Sr. No."].notna()]

        # Extract relevant columns
        relevant_columns = {col: df[col] for col in columns_to_fetch if col in df.columns}
        
        if relevant_columns:
            extracted_data = pd.DataFrame(relevant_columns)
            all_data.append(extracted_data)

    # Combine all data into a single DataFrame, ensuring the columns are in the correct order
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
    else:
        combined_df = pd.DataFrame(columns=columns_to_fetch)

    # Write combined data to a new Excel file
    output_file_name = f"processed_{os.path.basename(input_file_name)}"
    
    with BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            combined_df.to_excel(writer, sheet_name='Excel_Integrator_Output', index=False)
            writer.close()
        output.seek(0)
        s3_client.upload_fileobj(output, bucket_name, f"output/{output_file_name}")

    return f"output/{output_file_name}"

def main():
    st.title("Excel Integrator")
    st.write("Upload an Excel file to process")

    uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx"])

    if uploaded_file is not None:
        file_name = uploaded_file.name.replace(" ", "_")
        input_file_name = f"input/{file_name}"
        print(input_file_name)
        
        # Generate presigned URL
        upload_url = generate_presigned_url(bucket_name, input_file_name)
        print(upload_url)
        
        # Upload file using presigned URL
        upload_to_s3_via_presigned_url(upload_url, uploaded_file.getvalue())
        st.write(f"Uploaded {file_name} to S3 bucket {bucket_name}/input")
        
        # Read and display preview of the first few rows of the first sheet
        first_sheet_name, first_sheet_df = read_excel_preview_from_s3(bucket_name, input_file_name)
        st.write(f"Preview of the first few rows of the {first_sheet_name}:")
        st.dataframe(first_sheet_df.head(10))

        # Ask the user to specify the header row
        header_row_index = st.number_input("Specify the row number to be considered as header (0-indexed):", min_value=0, max_value=len(first_sheet_df)-1, step=1)

        # Extract columns from the specified header row
        columns_from_header = first_sheet_df.iloc[header_row_index].tolist()

        # Provide a dropdown to select columns
        st.write("Select columns that you want to fetch from all the sheets:")
        selected_columns = st.multiselect("Columns", columns_from_header)
        
        if st.button("Process File"):
            st.write("Columns to fetch:", selected_columns)
            output_file_name = load_and_process_excel(bucket_name, input_file_name, selected_columns, header_row_index)
            st.write(f"Processed file saved to {output_file_name}")
            # Generate presigned URL for downloading the processed file
            download_url = generate_presigned_url_for_download(bucket_name, output_file_name)
            st.markdown(f"[Download processed file]({download_url})")

def lambda_handler(event, context):
    os.environ['STREAMLIT_SERVER_PORT'] = '8080'
    os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
    main()

if __name__ == "__main__":
    main()
