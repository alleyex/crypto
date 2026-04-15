from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

_TEMPLATE_DIR = Path(__file__).parent / "templates"
_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATE_DIR)),
    autoescape=select_autoescape(disabled_extensions=("html",)),
)

def render_admin_page() -> str:
    from app.core.settings import DEFAULT_PIPELINE_ORCHESTRATION
    from app.core.settings import DEFAULT_STRATEGY_NAME
    from app.strategy.registry import list_registered_strategies

    strategy_names = list_registered_strategies()
    pipeline_orchestrations = ["direct", "queue_dispatch", "queue_drain", "queue_batch"]

    template = _env.get_template("admin.html")
    return template.render(
        strategy_names=strategy_names,
        closed_trade_strategy_names=["all", *strategy_names],
        pipeline_orchestrations=pipeline_orchestrations,
        default_strategy_name=DEFAULT_STRATEGY_NAME,
        default_pipeline_orchestration=DEFAULT_PIPELINE_ORCHESTRATION,
    )
