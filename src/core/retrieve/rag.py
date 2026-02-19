import environ
from datapizza.clients.openai import OpenAIClient
from datapizza.core.models import PipelineComponent
from datapizza.embedders.openai import OpenAIEmbedder
from datapizza.modules.prompt import ChatPromptTemplate
from datapizza.modules.rewriters import ToolRewriter
from datapizza.pipeline import DagPipeline

from src.core.db import PgVectorStore

env = environ.Env()


class RagPipeline(DagPipeline):
    def __init__(self, **kwargs):
        super().__init__()
        self.api_key = env.str("OPENAI_API_KEY")
        self.embedding_model = env.str("EMBEDDING_MODEL_NAME")
        self.openai_client = OpenAIClient(api_key=self.api_key)
        self.embedder = OpenAIEmbedder(api_key=self.api_key, model_name=self.embedding_model)
        self.query_rewriter = ToolRewriter(
            client=self.openai_client,
            system_prompt=(
                "Rewrite the user query for CV retrieval.\n"
                "Preserve every hard constraint exactly (skills, years of experience, seniority, location, education).\n"
                "Never relax constraints and never invert numeric ranges.\n"
                "If query says '<3 years' or 'less than 3 years', keep that exact limit.\n"
                "Return one concise retrieval query only."
            ),
        )
        self.vector_store = PgVectorStore()

        self.prompt_template = ChatPromptTemplate(
            user_prompt_template="User question: {{user_prompt}}\n:",
            retrieval_prompt_template="Retrieved content:\n{% for chunk in chunks %}{{ chunk.text }}\n{% endfor %}",
        )

        self.add_module("rewriter", self.query_rewriter)
        self.add_module("embedder", self.embedder)
        self.add_module(
            "retriever", self.vector_store
        )  # TODO FIX LIBRARY Expected type 'PipelineComponent', got 'PgVectorStore' instead --- Funziona
        self.add_module("retrieve_cvs", self.prompt_template)
        # self.add_module("prompt", self.prompt_template)
        # self.add_module("generator", self.openai_client) #TODO FIX LIBRARY TYPING Expected type 'PipelineComponent', got 'OpenAIClient' instead --- FUNZIONA

        self.connect("rewriter", "embedder", target_key="text")
        self.connect("embedder", "retriever", target_key="query_vector")

        # self.connect("retriever", "prompt", target_key="chunks")
        # self.connect("prompt", "generator", target_key="memory")
