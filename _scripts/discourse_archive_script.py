#!/usr/bin/env python3
"""
This script does a basic archive of Discourse content by way of its API.

TODO: figure out how to handle post updates.

"""
import argparse
import urllib.request
import sys
import time
import os
import json
import functools
import datetime
from dataclasses import dataclass
from pathlib import Path
import numpy as np
from tqdm import tqdm
import re

import logging

loglevel = 'DEBUG' if os.environ.get('DEBUG') else 'INFO'
try:
    # If `rich` is installed, use pretty logging.
    from rich.logging import RichHandler
    logging.basicConfig(level=loglevel, datefmt="[%X]", handlers=[RichHandler()])
except ImportError:
    logging.basicConfig(level=loglevel)

log = logging.getLogger('archive')


parser = argparse.ArgumentParser(
    'discourse-archive',
    description='Create a basic content archive from a Discourse installation')
parser.add_argument(
    '-u', '--url', help='URL of the Discourse server',
    default=os.environ.get('DISCOURSE_URL', 'https://discourse.slicer.org/'))
parser.add_argument(
    '--debug', action='store_true', default=os.environ.get('DEBUG'))
parser.add_argument(
    '-t', '--target-dir', help='Target directory for the archive',
    default=Path(os.environ.get('TARGET_DIR', './archive')))


@functools.cache
def args():
    return parser.parse_args()


def http_get(path) -> str:
    log.debug("HTTP GET %s", path)
    backoff = 3

    while True:
        try:
            with urllib.request.urlopen(f"{args().url}{path}") as f:
                return f.read().decode()
        except Exception:
            time.sleep(backoff)
            backoff *= 2
            print(f"{args().url}{path}")
            print("backoff now set at: " + str(backoff))

            if backoff >= 256:
                log.exception('ratelimit exceeded, or something else wrong?')
                sys.exit(1)


def http_get_json(path) -> dict:
    try:
        return json.loads(http_get(path))
    except json.JSONDecodeError:
        log.warning("unable to decode JSON response from %r", path)
        raise


class PostSlug:
    @classmethod
    def id_from_filename(cls, name: str) -> int:
        return int(name.split('-', 1)[0])


@dataclass(frozen=True)
class PostTopic:
    id: int
    slug: str
    title: str


@dataclass(frozen=True)
class Post:
    id: int
    slug: str
    raw: dict

    def get_created_at(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.raw['created_at'])

    def save(self, dir: Path):
        """Write the raw post to disk."""
        idstr = str(self.id).zfill(10)
        username = self.raw['username'][:10]  # Truncate username if too long
        topic_slug = self.raw['topic_slug'][:50]  # Truncate topic slug if too long
        filename = f"{idstr}-{username}-{topic_slug}.json"
        
        # Ensure the filename length does not exceed a safe limit (e.g., 100 characters)
        max_filename_length = 100
        if len(filename) > max_filename_length:
            # Truncate filename to max length minus extension length
            filename = filename[:max_filename_length - 5] + '.json'
        
        folder_name = self.get_created_at().strftime('%Y-%m-%B')
        full_path = dir / folder_name / filename
        
        # Ensure the directory exists before trying to write the file
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        log.info("saving post %s to %s", self.id, full_path)
        full_path.write_text(json.dumps(self.raw, indent=2))

    def get_topic(self) -> PostTopic:
        return PostTopic(
            id=self.raw['topic_id'],
            slug=self.raw['topic_slug'],
            title=self.raw['topic_title'],
        )

    @classmethod
    def from_json(cls, j: dict) -> 'Post':
        return cls(
            id=j['id'],
            slug=j['topic_slug'],
            raw=j,
        )


@dataclass(frozen=True)
class Topic:
    id: int
    slug: str
    raw: dict
    markdown: str

    def get_created_at(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.raw['created_at'])
    
    def save_rendered(self, dir):
        """Write the rendered (.md) topic to disk."""
        dir = Path(dir)  # Ensure dir is a Path object
        date = str(self.get_created_at().date())
        slug_truncated = self.slug[:50]  # Truncate the slug if too long
        idstr = str(self.id).zfill(10)
        filename = f"{date}-{slug_truncated}-id{idstr}.md"
        
        # Ensure the filename length does not exceed a safe limit (e.g., 150 characters)
        max_filename_length = 150
        if len(filename) > max_filename_length:
            filename = filename[:max_filename_length - 3] + '.md'
        
        folder_name = self.get_created_at().strftime('%Y-%m-%B')
        full_path = dir / folder_name / filename  # Construct the path using Path objects
        full_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Construct the URL to the topic
        base_url = "https://discourse.slicer.org/"
        topic_url = f"{base_url}/t/{self.slug}/{self.id}"
        
        # Append the URL to the markdown content
        markdown = f"# {self.raw['title']}\n\n{self.markdown}\n\n[Link to the original post]({topic_url})"
        
        log.info("saving topic markdown %s to %s", self.id, full_path)
        full_path.write_text(markdown, encoding='utf-8')

    # def save_rendered(self, dir: Path):
    #     """Write the rendered (.md) topic to disk."""
    #     date = str(self.get_created_at().date())
    #     filename = f"{date}-{self.slug}-id{self.id}.md"
    #     folder_name = self.get_created_at().strftime('%Y-%m-%B')
    #     full_path = Path(os.path.join(dir, folder_name, filename))
    #     full_path.parent.mkdir(parents=True, exist_ok=True)
        
    #     # Construct the URL to the topic
    #     base_url = "https://discourse.slicer.org/"
    #     topic_url = f"{base_url}/t/{self.slug}/{self.id}"

    #     # Append the URL to the markdown content
    #     markdown = f"# {self.raw['title']}\n\n{self.markdown}\n\n[Link to the original post]({topic_url})"
        
    #     log.info("saving topic markdown %s to %s", self.id, full_path)
    #     full_path.write_text(markdown)


    def get_topic(self) -> PostTopic:
        return PostTopic(
            id=self.raw['topic_id'],
            slug=self.raw['topic_slug'],
            title=self.raw['topic_title'],
        )

    @classmethod
    def from_json(cls, t: dict, markdown: str) -> 'Topic':
        return cls(
            id=t['id'],
            slug=t['slug'],
            raw=t,
            markdown=markdown,
        )


def main() -> None:
    """
    Sync posts back to `metdata[last_sync_date] - 1 day`, and then save the rendered
    version of all topics associated with those posts.
    """
    target_dir = args().target_dir
    target_dir = Path(target_dir) if not isinstance(target_dir, Path) else target_dir

    (posts_dir := target_dir / 'posts').mkdir(parents=True, exist_ok=True)
    (topics_dir := target_dir / 'rendered-topics').mkdir(parents=True, exist_ok=True)

    metadata_file = target_dir / '.metadata.json'
    last_sync_date = None
    metadata = {}

    if metadata_file.exists():
        metadata = json.loads(metadata_file.read_text())
        last_sync_date = datetime.datetime.fromisoformat(metadata['last_sync_date'])

    if last_sync_date:
        # Resync over the last day to catch any post edits.
        last_sync_date -= datetime.timedelta(days=1)

    log.info("detected latest synced post date: %s", last_sync_date)

    topics_to_get = {}
    max_created_at = None
    last_created_at: datetime.datetime | None = None
    last_id: int | None = None

    posts = http_get_json('/posts.json')['latest_posts']
    no_new_posts = False

    while posts:
        log.info("processing %d posts", len(posts))
        for json_post in posts:
            try:
                post = Post.from_json(json_post)
            except Exception:
                log.warning("failed to deserialize post %s", json_post)
                raise
            last_created_at = post.get_created_at()

            if last_sync_date is not None:
                no_new_posts = last_created_at < last_sync_date
                if no_new_posts:
                    break

            post.save(posts_dir)

            if not max_created_at:
                # Set in this way because the first /post.json result returned will be
                # the latest created_at.
                max_created_at = post.get_created_at()

            last_id = post.id
            topic = post.get_topic()
            topics_to_get[topic.id] = topic

        if no_new_posts or last_id is not None and last_id <= 1:
            log.info("no new posts, stopping")
            break

        time.sleep(5)
        posts = http_get_json(
            f'/posts.json?before={last_id - 1}')['latest_posts']

        # Discourse implicitly limits the posts query for IDs between `before` and
        # `before - 50`, so if we don't get any results we have to kind of scan.
        while not posts and last_id >= 0:
            # This is probably off-by-one, but doesn't hurt to be safe.
            last_id -= 49
            posts = http_get_json(
                f'/posts.json?before={last_id}')['latest_posts']
            time.sleep(1)

    if max_created_at is not None:
        metadata['last_sync_date'] = max_created_at.isoformat()
        log.info("writing metadata: %s", metadata)
        metadata_file.write_text(json.dumps(metadata, indent=2))

    time.sleep(3)

    for topic in topics_to_get.values():
        data = http_get_json(f"/t/{topic.id}.json")
        body = http_get(f"/raw/{topic.id}")
        page_num = 2

        if not body:
            log.warning("could not retrieve topic %d markdown", topic.id)
            continue

        while (more_body := http_get(f"/raw/{topic.id}?page={page_num}")):
            body += f"\n{more_body}"

        t = Topic.from_json(data, body)
        t.save_rendered(topics_dir)
        log.info("saved topic %s (%s)", t.id, t.slug)

        time.sleep(0.3)


def collect_ids(directory):
    topics_list = []
    # Collect all json file paths first to establish tqdm progress bar
    json_files = [
        os.path.join(root, file)
        for root, dirs, files in os.walk(directory)
        for file in files if file.endswith('.json')
    ]

    # Process each file, displaying progress with tqdm
    for file_path in tqdm(json_files, desc="Processing JSON files"):
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            topics_list.append((data['topic_id'], data['topic_slug']))
    return np.array(topics_list)


def renderFromJSONs() -> None:

    directory = r'C:\repos\chat-langchain\_scripts\archive\posts'
    topics_dir = r'C:\repos\chat-langchain\_scripts\archive\rendered-topics'
    # topics_array = collect_ids(directory)
    # print(topics_array)

    # np.save(r'C:\repos\chat-langchain\_scripts\topics_array.npy', topics_array)

    data = np.load(r'C:\repos\chat-langchain\_scripts\topics_array.npy')

    # Convert list of lists to list of tuples to make them hashable
    data_tuples = [tuple(item) for item in data]

    # Use a set to remove duplicates
    unique_data_tuples = set(data_tuples)

    # Convert back to list of lists if necessary
    unique_data = [list(item) for item in unique_data_tuples]


    # establish which IDs have already been extracted
    id_list = []

    # Walk through the directory
    for root, dirs, files in os.walk(topics_dir):
        for file in files:
            # Check if the file is a Markdown file
            if file.endswith(".md"):
                # Use regex to find the pattern "-id" followed by numbers
                match = re.search(r'-id(\d+)\.md', file)
                if match:
                    file_id = match.group(1)  # Extract the ID using the regex match
                    print(file_id)  # Print the extracted ID
                    id_list.append(int(file_id))  # Convert ID to integer and append to list

    # Convert the list of IDs to a numpy array
    id_array = np.array(id_list)

    # print(id_array)

    # print(unique_data)

    # filtered_tuples = []

    # for id, topic_slug in tqdm(filtered_tuples):
    #     pass
        

    # Filter the list to exclude tuples where ID_INTEGER is in id_array
    filtered_tuples = [t for t in unique_data  if int(t[0]) not in id_array]

    print("Original number of topics: " + str(len(unique_data)) )
    print("Number of existing files: " + str(len(id_array )) )
    print("Remaining topics to scrap: " + str(len(filtered_tuples)) )

    for id, topic_slug in tqdm(filtered_tuples):
        data = http_get_json(f"/t/{topic_slug}/{id}.json")
        body = http_get(f"/raw/{id}")
        page_num = 2

        if not body:
            log.warning("could not retrieve topic %d markdown", id)
            continue

        while (more_body := http_get(f"/raw/{id}?page={page_num}")):
            body += f"\n{more_body}"

        t = Topic.from_json(data, body)
        t.save_rendered(topics_dir)
        log.info("saved topic %s (%s)", t.id, t.slug)

        time.sleep(2)


if __name__ == "__main__":
    renderFromJSONs()
