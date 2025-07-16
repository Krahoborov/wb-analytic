from .start import register_start_handlers
from .settings import register_settings_handlers
from .pnl import register_pnl_handlers
from .analytics import register_analytics_handlers
from .shops import register_shops_handlers
from .subscription import register_subscription_handlers
from .admin import register_admin_handlers
from .support import register_support_handlers
def register_all_handlers(dp):
    register_start_handlers(dp)
    register_settings_handlers(dp)
    register_pnl_handlers(dp)
    register_analytics_handlers(dp)
    register_shops_handlers(dp)
    register_subscription_handlers(dp)
    register_admin_handlers(dp)
    register_support_handlers(dp)
