# en el archivo enbios/main.py
from pathlib import Path
from typing import Optional

import click


def check_exists(ctx, param, value):
    if not Path(value).exists():
        raise click.BadParameter('Please provide a valid path')
    return value


@click.command()
@click.option('--config', prompt='Config file (json file)', help='The configuration file.', callback=check_exists)
@click.option("--output", prompt="Output", help="Output file.")
@click.option('--scenarios', multiple=True, help='List of scenarios.')
def main_cli(config: str, output: str, scenarios: Optional[list[str]]):
    exp = Experiment(config, scenarios)
    exp.run()
    exp.results_to_csv(output)


from enbios.base.experiment import Experiment

if __name__ == '__main__':
    main_cli()
