from environs import Env

env = Env()
env.read_env()

PROCESS_PUBSUB_MESSAGES_IN_BACKGROUND = env.bool("MAX_OBSERVATIONS_TO_SEND", False)
MAX_OBSERVATIONS_TO_SEND = env.int("MAX_OBSERVATIONS_TO_SEND", 100)
