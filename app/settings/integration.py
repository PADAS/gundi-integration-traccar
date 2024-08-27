from environs import Env

env = Env()
env.read_env()

MAX_OBSERVATIONS_TO_SEND = env.int("MAX_OBSERVATIONS_TO_SEND", 100)
