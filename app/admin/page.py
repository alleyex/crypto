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

    strategy_options = "\n".join(
        (
            f'              <option value="{name}"'
            + (' selected' if name == DEFAULT_STRATEGY_NAME else '')
            + f">{name}</option>"
        )
        for name in list_registered_strategies()
    )
    closed_trade_strategy_options = "\n".join(
        ['              <option value="all">all</option>']
        + [f'              <option value="{name}">{name}</option>' for name in list_registered_strategies()]
    )
    pipeline_orchestration_options = "\n".join(
        (
            f'              <option value="{name}"'
            + (' selected' if name == DEFAULT_PIPELINE_ORCHESTRATION else '')
            + f">{name}</option>"
        )
        for name in ("direct", "queue_dispatch", "queue_drain", "queue_batch")
    )

    template = _env.get_template("admin.html")
    return template.render(
        strategy_options=strategy_options,
        closed_trade_strategy_options=closed_trade_strategy_options,
        pipeline_orchestration_options=pipeline_orchestration_options,
        default_strategy_name=DEFAULT_STRATEGY_NAME,
    )
