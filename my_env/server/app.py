from openenv.core.env_server import create_fastapi_app
from ..models import DataWranglerAction, DataWranglerObservation
from .environment import DataWranglerEnvironment

# This one line auto-generates the /ws, /reset, /step, /state, and /health endpoints!
app = create_fastapi_app(DataWranglerEnvironment, DataWranglerAction, DataWranglerObservation)