def describe_exception(e):
    msg = str(e)
    cause = type(e.__cause__).__name__
    description = type(e).__name__

    if msg:
        description += ": {}".format(msg)

    if cause:
        description += ", cause: {}".format(cause)

    return description
