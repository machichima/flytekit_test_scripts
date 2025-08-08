import click

@click.command()
@click.option(
    "--default-resources",
    nargs=3,
    help="Resource settings like cpu=1 mem=2Gi gpu=1",
)
@click.option("--other-arg", default="default", help="Some other arg")
def cli(default_resources, other_arg):
    resource_dict = dict(kv.split("=", 1) for kv in default_resources)
    click.echo(f"Resources: {resource_dict}")
    click.echo(f"Other arg: {other_arg}")


if __name__ == "__main__":
    cli()
