import argparse
import os


def get_arg(key):
    if key in vars(args).keys():
        return vars(args)[key]
    else:
        return None


def get_master():
    return f"{get_arg('master')}"


def get_jars():
    return f"{get_arg('jars')}"


def get_repositories():
    return f"{get_arg('repositories')}"


def get_packages():
    return f"{get_arg('packages').rstrip(',')}"


def get_conf():
    res = ""
    for c in get_arg('conf'):
        res += f" --conf {c[0]}"
    return res


if __name__ == "__main__":
    pyspark_submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
    parser = argparse.ArgumentParser()
    parser.add_argument(f"--conf", action='append', nargs='+')
    parser.add_argument(f"--master")
    parser.add_argument(f"--jars")
    parser.add_argument(f"--repositories")
    parser.add_argument(f"--packages")
    args, other_args = parser.parse_known_args(pyspark_submit_args.split())

    parser = argparse.ArgumentParser(description='Process Spark params')
    parser.add_argument('--master', dest='master', action='store_true')
    parser.add_argument('--conf', dest='conf', action='store_true')
    parser.add_argument('--jars', dest='jars', action='store_true')
    parser.add_argument('--packages', dest='packages', action='store_true')
    parser.add_argument('--repositories', dest='repositories', action='store_true')
    args_cmd = parser.parse_args()
    if args_cmd.master:
        print(get_master())
    elif args_cmd.conf:
        print(get_conf())
    elif args_cmd.jars:
        print(get_jars())
    elif args_cmd.packages:
        print(get_packages())
    elif args_cmd.repositories:
        print(get_repositories())
