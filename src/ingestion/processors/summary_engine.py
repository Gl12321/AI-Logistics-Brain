from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.core.logger import setup_logger

logger = setup_logger("SUMMARY_ENGINE")


class SummaryEngine:
    def __init__(self, summarizer):
        self.summary_chain = summarizer.get_summary_chain()
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=20000,
            chunk_overlap=1000,
            length_function=len
        )

    def summarize(self, text: str) -> str:
        cur_text = text
        count_iterations = 1

        while True:
            chunks = self.text_splitter.split_text(cur_text)

            if len(chunks) == 1:
                logger.info("Text summarized")
                return self.summary_chain.invoke({"text": chunks[0]})

            cur_text = []
            for chunk in chunks:
                cur_text.append(self.summary_chain.invoke(chunk))
            cur_text = "\n\n".join(cur_text)

            count_iterations += 1
            if count_iterations > 5:
                logger.warring("Max iterations reached")
                break

        return cur_text