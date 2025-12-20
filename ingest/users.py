import logging
from ingest.base import BitSightIngestBase


class UsersIngest(BitSightIngestBase):

    def run(self):
        logging.info("Ingesting users")

        for user in self.paginate("/ratings/v2/users"):
            self.process_user(user)

    def process_user(self, user: dict):
        # DB write happens here
        logging.debug("User GUID: %s", user.get("guid"))
