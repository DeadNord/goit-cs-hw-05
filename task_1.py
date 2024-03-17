import argparse
import asyncio
from pathlib import Path
import shutil
import logging

# Setting up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class ArgumentParser:
    @staticmethod
    def parse_arguments():
        """Parses command line arguments."""
        parser = argparse.ArgumentParser(
            description="Organize files by their extension"
        )
        parser.add_argument("--source", "-s", required=True, help="Source folder path")
        parser.add_argument(
            "--dest", "-d", required=True, help="Destination folder path"
        )
        return parser.parse_args()


class FileOrganizer:
    def __init__(self, source_folder, output_folder):
        self.source_folder = Path(source_folder)
        self.output_folder = Path(output_folder)

    async def read_folder(self, folder: Path):
        """Asynchronously reads all files in the given folder and its subfolders."""
        try:
            # Convert the rglob result to a list since it's not asynchronous
            files = list(folder.rglob("*.*"))
            for file in files:
                await self.copy_file(file)
        except Exception as e:
            logging.error(f"Error reading folder {folder}: {e}")

    async def copy_file(self, file: Path):
        """Asynchronously copies a file to the corresponding subfolder in the target folder based on its extension."""
        try:
            extension = file.suffix[1:]  # Remove the dot from the file extension
            dest_folder = self.output_folder / extension
            dest_folder.mkdir(exist_ok=True)  # Create the folder if it does not exist
            dest_file = dest_folder / file.name
            if not dest_file.exists():
                await asyncio.to_thread(shutil.copy, file, dest_file)
                logging.info(f"Copied {file} to {dest_file}")
            else:
                logging.warning(f"File {dest_file} already exists. Skipping.")
        except Exception as e:
            logging.error(f"Error copying file {file}: {e}")


async def main():
    try:
        args = ArgumentParser.parse_arguments()
        organizer = FileOrganizer(args.source, args.dest)
        await organizer.read_folder(organizer.source_folder)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


# Execute the main asynchronous function
if __name__ == "__main__":
    asyncio.run(main())
