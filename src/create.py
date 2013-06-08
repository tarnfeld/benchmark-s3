import argparse
import sys
import logging
import atexit
from os import environ
from datetime import datetime, timedelta
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from threading import Thread, Event
from Queue import Queue, Empty as EmptyException


# Create a logger
logger = logging.getLogger("benchmark-s3.create")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def parse_arguments():
    """Return the parsed aruguments"""

    parser = argparse.ArgumentParser(description="Create a large number of \
                                                  dated files in S3.")
    parser.add_argument("--verbose", action="store_true", help="Verose logging.")
    parser.add_argument('--bucket', help="S3 bucket to use.")
    parser.add_argument('--aws_key', default=environ.get("AWS_ACCESS_KEY_ID"),
                                     help="AWS access key ID.")
    parser.add_argument('--aws_secret', default=environ.get("AWS_SECRET_ACCESS_KEY"),
                                        help="AWS secret key.")
    parser.add_argument('--interval', default=600, type=int,
                                      help="Number of seconds to separate each \
                                            file.")
    parser.add_argument('--days', default=10, type=int,
                                  help="Number of days to go back creating \
                                        files.")
    parser.add_argument('--format', default="%Y%m%d%H%M%S",
                                    help="Date format for the files.")
    parser.add_argument('--concurrency', default=2, type=int,
                                         help="Number of concurrent API calls \
                                               to S3.")

    return parser.parse_args(sys.argv[1:])


class S3KeyThread(Thread):

    def __init__(self, queue, timeout, num):

        super(S3KeyThread, self).__init__()
        self.queue = queue
        self.timeout = timeout
        self.num = num
        self.kill = Event()

    def run(self):

        try:
            while True:
                if self.kill.isSet():
                    logger.debug("Thread exiting %d" % (self.num))
                    break

                key = self.queue.get(timeout=self.timeout)
                key.set_contents_from_string("")

                logger.debug("Uploaded key %s" % (key.name))

                self.queue.task_done()
        except EmptyException:
            logger.info("Thread %d finished the queue" % (self.num))


def main(bucket, aws_key, aws_secret, interval, days, format, concurrency):

    assert bucket
    assert aws_key and aws_secret
    assert interval > 0
    assert format

    connection = S3Connection(aws_key, aws_secret)

    # Create the bucket if it doesn't exist
    bucket = connection.create_bucket(bucket)

    # Create a test key
    test = Key(bucket=bucket, name="test_key")
    test.set_contents_from_string("123", reduced_redundancy=True)
    test.delete()

    # Create the two date ranges
    from_date = datetime.now() - timedelta(days=days)
    to_date = datetime.now()

    # Create a queue to collect the files
    queue = Queue()

    # Create some children to make the S3 requests
    threads = []
    for i in range(concurrency):
        logger.info("Spawning thread %d" % (i))
        thread = S3KeyThread(queue=queue, timeout=1, num=i)
        thread.daemon = True
        threads.append(thread)
        thread.start()

    # Kill the threads if asked to
    def handler():
        for thread in threads:
            logger.debug("Killing thread %d" % (thread.num))
            thread.kill.set()
    atexit.register(handler)

    # Put a shit load of items on the queue
    current = from_date
    while current <= to_date:
        time = current.strftime(format)
        k = Key(bucket=bucket, name=time + ".log")

        queue.put(k)
        current += timedelta(seconds=interval)

    alive = True
    while alive:
        for thread in threads:
            if thread.isAlive():
                alive = True


if __name__ == "__main__":

    args = parse_arguments()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # GOGOGO
    main(bucket=args.bucket,
         aws_key=args.aws_key,
         aws_secret=args.aws_secret,
         interval=args.interval,
         days=args.days,
         format=args.format,
         concurrency=args.concurrency)
