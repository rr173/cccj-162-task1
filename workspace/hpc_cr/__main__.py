"""``python -m hpc_cr`` 启动 HTTP 服务。"""

from .api import serve_forever

if __name__ == "__main__":
    serve_forever()
