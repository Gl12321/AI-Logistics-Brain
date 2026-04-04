from langchain_community.llms import LlamaCpp
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableSerializable

from src.core.config import get_settings
from src.core.logger import setup_logger

settings = get_settings()
logger = setup_logger("SUMMARIZER")


class Summarizer:
    def __init__(self):
        self.summarizer_config = settings.MODELS["summarizer"]
        self.llm = LlamaCpp(
            model_path=str(self.summarizer_config["cache_path"] / self.summarizer_config["filename"]),
            n_ctx=self.summarizer_config["context_window"],
            n_gpu_layers=-1,
            tensor_split=self.summarizer_config["tensor_split"],
            temperature=self.summarizer_config["temperature"],
            verbose=False
        )

        logger.info("Model loaded")

    def get_summary_chain(self, custom_prompt: str = None) -> RunnableSerializable[dict, str]:
        default_prompt = (
            "You are a professional financial analyst. "
            "Make detail summarize the following text from a 10-K report. "
            "Provide only the essence of the business operations. \n\n"
            "TEXT: {text}\n\n"
            "SUMMARY:"
        )

        prompt_template = ChatPromptTemplate.from_template(
            custom_prompt or default_prompt
        )

        chain = prompt_template | self.llm | StrOutputParser()

        return chain