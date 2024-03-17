import string
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class TextFetcher:
    """Class for fetching text from a URL."""

    @staticmethod
    def fetch(url: str) -> str:
        """Fetches text from a given URL.

        Args:
            url (str): URL to fetch text from.

        Returns:
            str: Text retrieved from the URL, None if an error occurs.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP errors
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching URL {url}: {e}")
            return None


class TextProcessor:
    """Class for processing text and executing MapReduce."""

    @staticmethod
    def remove_punctuation(text: str) -> str:
        """Removes punctuation from the text.

        Args:
            text (str): Text to remove punctuation from.

        Returns:
            str: Text without punctuation.
        """
        return text.translate(str.maketrans("", "", string.punctuation))

    @staticmethod
    def map_function(word: str) -> tuple:
        """Map function for MapReduce, maps each word to a tuple (word, 1).

        Args:
            word (str): A word from the text.

        Returns:
            tuple: (word, 1) where word is in lowercase.
        """
        return word.lower(), 1

    @staticmethod
    def shuffle_and_reduce(mapped_values) -> Counter:
        """Combines and counts similar words using the MapReduce shuffle and reduce phases.

        Args:
            mapped_values: Mapped values from the map_function.

        Returns:
            Counter: A Counter dict of words and their frequencies.
        """
        result = Counter()
        for word, count in mapped_values:
            result[word] += count
        return result

    def process_text(self, text: str) -> Counter:
        """Processes the given text and performs the MapReduce algorithm.

        Args:
            text (str): Text to process.

        Returns:
            Counter: A Counter dict containing words and their frequencies.
        """
        text = self.remove_punctuation(text)
        words = text.split()

        with ThreadPoolExecutor() as executor:
            mapped_values = executor.map(self.map_function, words)

        return self.shuffle_and_reduce(mapped_values)


class DataVisualizer:
    """Class for data visualization."""

    @staticmethod
    def visualize(word_counts: Counter, top_n: int = 10):
        """Visualizes the top-N words by frequency.

        Args:
            word_counts (Counter): Counter dict containing words and their frequencies.
            top_n (int): Number of top words to visualize.
        """
        top_words = word_counts.most_common(top_n)
        words, counts = zip(*top_words)
        plt.figure(figsize=(10, 8))
        plt.barh(range(len(words)), counts, color="skyblue")
        plt.yticks(range(len(words)), words)
        plt.xlabel("Frequency")
        plt.ylabel("Words")
        plt.title("Top 10 Most Frequent Words")
        plt.gca().invert_yaxis()
        plt.show()


if __name__ == "__main__":
    URL = "https://gutenberg.net.au/ebooks01/0100021.txt"
    text = TextFetcher.fetch(URL)

    if text:
        processor = TextProcessor()
        word_counts = processor.process_text(text)
        DataVisualizer.visualize(word_counts, 10)
    else:
        logging.error("Failed to proceed due to an error in fetching the text.")
