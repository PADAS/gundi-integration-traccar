from environs import Env

env = Env()
env.read_env()

MAX_OBSERVATIONS_TO_SEND = env.int("MAX_OBSERVATIONS_TO_SEND", 100)
TRACCAR_ACTIONS_PUBSUB_TOPIC = env.str("TRACCAR_ACTIONS_PUBSUB_TOPIC", "traccar-actions-topic")
