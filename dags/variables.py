from airflow.sdk import Variable


def ollama_endpoint():
    return Variable.get('OLLAMA_ENDPOINT')

def get_file_endpoint():
    return Variable.get('GET_FILE_ENDPOINT')

def metadata_endpoint():
    return Variable.get('METADATA_ENDPOINT')
