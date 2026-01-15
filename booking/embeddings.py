import os
from functools import lru_cache

from openai import OpenAI

EMBED_MODEL = os.environ.get("BOOKING_EMBED_MODEL", "text-embedding-3-small")


def _embed_texts(client, texts):
    response = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [item.embedding for item in response.data]


@lru_cache(maxsize=128)
def _embed_query_cached(model, query):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    response = client.embeddings.create(model=model, input=[query])
    return response.data[0].embedding
