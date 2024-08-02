import os
import requests
import json
import schedule 
import time 
import copy
from tenacity import retry, wait_random_exponential, stop_after_attempt 
from dotenv import load_dotenv
import openai
from openai import AzureOpenAI
from langchain.text_splitter import TokenTextSplitter, RecursiveCharacterTextSplitter
from gbb_ai.sharepoint_data_extractor import SharePointDataExtractor
from azure.core.credentials import AzureKeyCredential  
from azure.search.documents import SearchClient  
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (  
    CorsOptions,
    HnswParameters,  
    HnswVectorSearchAlgorithmConfiguration,
    SimpleField,
    SearchField,  
    ComplexField,
    SearchFieldDataType,  
    SearchIndex,  
    VectorSearch,  
    VectorSearchAlgorithmKind,  
    VectorSearchProfile,
    SearchableField,
    SemanticConfiguration,
    PrioritizedFields,
    SemanticField,
    SemanticSettings,
)

# Load environment variables from .env file
load_dotenv()

# Set up SharePoint Data Extractor
client_scrapping = SharePointDataExtractor()
client_scrapping.load_environment_variables_from_env_file()
client_scrapping.msgraph_auth()

# Set up OpenAI
openai.api_key = os.environ["OPEN_API_KEY"]
openai.api_base = os.environ["OPEN_API_BASE"]
openai.api_type = "azure"  
openai.api_version = "2023-05-15"
model = os.environ["OPEN_API_MODEL"]
client = AzureOpenAI(
    api_version=openai.api_version,
    azure_endpoint=openai.api_base,
    api_key=openai.api_key
)

def get_current_index_name():
    endpoint = os.getenv("SEARCH_SERVICE_ENDPOINT")
    admin_client = SearchIndexClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(os.getenv("SEARCH_ADMIN_API_KEY")),
    )
    
    indexes = list(admin_client.list_index_names())
    if "index_1" in indexes:
        return "index_1"
    elif "index_2" in indexes:
        return "index_2"
    else:
        return "index_1"  # Default to index_1 if neither exists

def get_new_index_name():
    current_index = get_current_index_name()
    return "index_2" if current_index == "index_1" else "index_1"

def create_index(index_name):
    print(f"Creating index: {index_name}")
    endpoint = os.getenv("SEARCH_SERVICE_ENDPOINT")
    admin_client = SearchIndexClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(os.getenv("SEARCH_ADMIN_API_KEY")),
    )

    # Define index fields
    fields = [
        SimpleField(name="id", type=SearchFieldDataType.String, filterable=True, sortable=True, key=True),
        SimpleField(name="doc_id", type=SearchFieldDataType.String, filterable=True, facetable=True, sortable=True, key=False),
        SimpleField(name="chunk_id", type=SearchFieldDataType.Int32, filterable=True, sortable=True, key=False),
        SearchField(name="name", type=SearchFieldDataType.String, filterable=True, sortable=True, analyzer_name="en.microsoft"),
        SimpleField(name="created_datetime", type=SearchFieldDataType.DateTimeOffset, facetable=True, filterable=True, sortable=True),
        SearchField(name="created_by", type=SearchFieldDataType.String, filterable=True, sortable=True),
        SimpleField(name="size", type=SearchFieldDataType.Int32, facetable=True, filterable=True, sortable=True),
        SimpleField(name="last_modified_datetime", type=SearchFieldDataType.DateTimeOffset, facetable=True, filterable=True, sortable=True),
        SearchField(name="last_modified_by", type=SearchFieldDataType.String, filterable=True, sortable=True),
        SimpleField(name="source", type=SearchFieldDataType.String),
        SearchField(name="content", type=SearchFieldDataType.String, analyzer_name="en.microsoft"),
        SearchField(name="contentVector", hidden=False, type=SearchFieldDataType.Collection(SearchFieldDataType.Single), searchable=True, vector_search_dimensions=1536, vector_search_profile="myHnswProfile"), 
        SearchField(name="read_access_entity", type=SearchFieldDataType.Collection(SearchFieldDataType.String), searchable=True, filterable=True),
        SearchField(name="metadata_main", type=SearchFieldDataType.String, filterable=True, facetable=True, sortable=True, analyzer_name="en.microsoft"),
        SearchField(name="metadata_subfolder", type=SearchFieldDataType.String, filterable=True, facetable=True, sortable=True, analyzer_name="en.microsoft"),
    ]

    # Configure CORS options
    cors_options = CorsOptions(allowed_origins=["*"], max_age_in_seconds=60)
    scoring_profiles = []
    suggester = [{"name": "sg", "source_fields": ["name"]}]

    # Configure vector search
    vector_search = VectorSearch(  
        algorithms=[  
            HnswVectorSearchAlgorithmConfiguration(  
                name="myHnsw",  
                kind=VectorSearchAlgorithmKind.HNSW,  
                parameters=HnswParameters(  
                    m=4,  
                    ef_construction=400,  
                    ef_search=1000,  
                    metric="cosine",  
                ),  
            )
        ],  
        profiles=[  
            VectorSearchProfile(  
                name="myHnswProfile",  
                algorithm="myHnsw",  
            ),   
        ],  
    ) 

    # Configure semantic search
    semantic_config = SemanticConfiguration(
        name="my-semantic-config",
        prioritized_fields=PrioritizedFields(
            title_field=SemanticField(field_name="source"),
            prioritized_content_fields=[SemanticField(field_name="content")],
            prioritized_keywords_fields=[SemanticField(field_name="read_access_entity")]
        )
    )
    semantic_settings = SemanticSettings(configurations=[semantic_config])

    index = SearchIndex(
        name=index_name,
        fields=fields,
        scoring_profiles=scoring_profiles,
        suggesters=suggester,
        cors_options=cors_options,
        vector_search=vector_search,
        semantic_settings=semantic_settings
    )

    # Create the index
    try:
        result = admin_client.create_index(index)
        print(f"Index {result.name} created")
        return True
    except Exception as ex:
        print(f"Error creating index: {ex}")
        return False

def delete_index(index_name):
    print(f"Deleting index: {index_name}")
    endpoint = os.getenv("SEARCH_SERVICE_ENDPOINT")
    admin_client = SearchIndexClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(os.getenv("SEARCH_ADMIN_API_KEY")),
    )

    try:
        admin_client.delete_index(index_name)
        print(f"Index {index_name} deleted")
        return True
    except Exception as ex:
        print(f"Error deleting index: {ex}")
        return False

def get_folders(url, folder_list = ['/']):
    headers = {'Authorization': 'Bearer ' + client_scrapping.access_token}
    response = requests.get(url, headers=headers)
    items = response.json()

    if 'value' not in items:
        return folder_list

    for item in items['value']:
        if 'folder' in item:
            subfolder_url = url + '/' + item['name'] + '/children'
            folder_val = subfolder_url[subfolder_url.index('/drive/root')+11:].replace('/children','') + '/'
            folder_list.append(folder_val)
            get_folders(subfolder_url, folder_list)

    return folder_list

def get_custom_list(url, custom_list = ['/']):
    headers = {'Authorization': 'Bearer ' + client_scrapping.access_token}
    response = requests.get(url, headers=headers)
    items = response.json()

    if 'value' not in items:
        return None
    filtered_list = [item for item in items['value'] if item["name"] not in ("wte", "Shared Documents", "Access Requests")]
    filtered_documentLibrary = [item for item in filtered_list if item["list"]["template"] == "documentLibrary"]
    filtered_named_list=[item for item in filtered_documentLibrary if item["displayName"] == "Helena_docs"]
    print("filtered_named_list:", filtered_named_list)
    return filtered_named_list

def divide_chunks(l, n):  
    for i in range(0, len(l), n):   
        yield l[i:i + n]  

@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
def generate_embeddings(text):
    response = client.embeddings.create(
        input=text,
        model=model
    )
    return response.data[0].embedding

def extract_main_folder(source_path):
    parts = source_path.split('/')
    try:
        helena_index = parts.index('Helena_docs')
        return parts[helena_index + 1] if helena_index + 1 < len(parts) else ''
    except ValueError:
        return ''

def extract_subfolder(source_path):
    parts = source_path.split('/')
    try:
        helena_index = parts.index('Helena_docs')
        return parts[helena_index + 2] if helena_index + 2 < len(parts) else ''
    except ValueError:
        return ''

def execute_index(index_name):
    print(f"Executing index: {index_name}")
    target_directory = r"C:\sharepoint-index-graphapi"

    if os.path.exists(target_directory):
        os.chdir(target_directory)
        print(f"Directory changed to {os.getcwd()}")
    else:
        print(f"Directory {target_directory} does not exist.")
    
    endpoint = os.environ["SEARCH_SERVICE_ENDPOINT"]
    search_client = SearchClient(
        endpoint=endpoint,
        index_name=index_name,
        credential=AzureKeyCredential(os.environ["SEARCH_ADMIN_API_KEY"]),
    )

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1024*4,
        chunk_overlap=102*4
    )

    client_scrapping.load_environment_variables_from_env_file()
    client_scrapping.msgraph_auth()

    site_id = client_scrapping.get_site_id(
        site_domain=os.environ["SITE_DOMAIN"], site_name=os.environ["SITE_NAME"]
    )

    drive_id = client_scrapping.get_drive_id(site_id)

    n = 100  # max batch size (number of docs) to upload at a time
    total_docs_uploaded = 0

    print("Fetching Custom Lists")

    lists_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists'  
    custom_lists = get_custom_list(lists_url) 
    print("custom_lists length:", len(custom_lists))
    
    for custom_list in custom_lists:
        selected_files_content = client_scrapping.retrieve_custom_list_files_content(
                site_domain=os.environ["SITE_DOMAIN"],
                site_name=os.environ["SITE_NAME"],
                file_formats=["docx", "pdf", "csv", "xlsx", "pptx"],
                list=custom_list,
        )
        
        if selected_files_content == None:
            print("No documents found in this folder")
        else:
            chunked_content_docs = []
            sfc_counter = 0
            for sfc_counter in range(len(selected_files_content)):
                chunked_content = text_splitter.split_text(selected_files_content[sfc_counter]['content'])
                chunk_counter = 0
                for cc in chunked_content:
                    json_data = copy.deepcopy(selected_files_content[sfc_counter]) 
                    json_data['content'] = chunked_content[chunk_counter]
                    json_data['contentVector'] = generate_embeddings(json_data['content'])
                    json_data['doc_id'] = json_data['id']
                    json_data['id'] = json_data['id'] + "-" + str(chunk_counter)
                    json_data['chunk_id'] = chunk_counter
                    json_data['read_access_entity'] = json_data['read_access_entity']
                    json_data['metadata_main'] = extract_main_folder(json_data['source'])
                    json_data['metadata_subfolder'] = extract_subfolder(json_data['source'])
                    chunk_counter += 1
                    chunked_content_docs.append(json_data)
                sfc_counter += 1
                
            total_docs = len(chunked_content_docs)
            total_docs_uploaded += total_docs
            print(f"Total Documents to Upload: {total_docs}")

            for documents_chunk in divide_chunks(chunked_content_docs, n):  
                try:
                    print(f"Uploading batch of {len(documents_chunk)} documents...")
                    result = search_client.upload_documents(documents=documents_chunk)
                    for res in result:
                        print("Upload of new document succeeded: {}".format(res.succeeded))
                except Exception as ex:
                    print("Error in multiple documents upload: ", ex)
                    return False

    print(f"Upload of {total_docs_uploaded} documents complete.")
    return True

def search_index(index_name):
    endpoint = os.environ["SEARCH_SERVICE_ENDPOINT"]
    search_client = SearchClient(
        endpoint=endpoint,
        index_name=index_name,
        credential=AzureKeyCredential(os.environ["SEARCH_ADMIN_API_KEY"]),
    )

    results = search_client.search(search_text="*", select="id,content,metadata_main,metadata_subfolder")
    finalResult = []
    
    with open('C:\sharepoint-index-graphapi\searchResult.json', 'w') as outfile:
        for result in results:
            finalResult.append(result)
        json.dump(finalResult, outfile, indent=4)

def main():
    current_index = get_current_index_name()
    new_index = get_new_index_name()

    print(f"Current index: {current_index}")
    print(f"New index to be created: {new_index}")

    # Create the new index
    if create_index(new_index):
        # Execute indexing on the new index
        if execute_index(new_index):
            # If indexing was successful, delete the old index
            if delete_index(current_index):
                print(f"Successfully switched from {current_index} to {new_index}")
            else:
                print(f"Warning: Failed to delete old index {current_index}")
        else:
            print(f"Error: Indexing failed for {new_index}")
            delete_index(new_index)  # Clean up the new index if indexing failed
    else:
        print(f"Error: Failed to create new index {new_index}")

# Schedule the main function to run every 40 minutes
schedule.every(15).minutes.do(main)

if __name__ == "__main__":
    main()  # Run immediately on script start
    while True:
        schedule.run_pending()
        time.sleep(1)