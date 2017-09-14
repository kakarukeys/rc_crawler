def describe_exception(e):
    msg = str(e)
    description = type(e).__name__

    if msg and msg != "None":
        description += ": {}".format(msg)

    if e.__cause__:
        description += ", cause: {}".format(type(e.__cause__).__name__)

    return description
