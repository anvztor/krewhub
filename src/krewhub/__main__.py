import uvicorn

from krewhub.app import create_app
from krewhub.config import get_settings


def main() -> None:
    settings = get_settings()
    app = create_app()
    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
