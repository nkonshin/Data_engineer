import pytest
from fastapi.testclient import TestClient
from backend.main import app

client = TestClient(app)


def test_upload_file_csv(tmp_path):
    content = "id,name,age\n1,Test,20\n"
    file_path = tmp_path / "test.csv"
    file_path.write_text(content)

    with open(file_path, "rb") as f:
        response = client.post("/upload-file/", files={"file": ("test.csv", f, "text/csv")})
    assert response.status_code == 200
    data = response.json()
    assert "pipeline_id" in data
    assert "recommendations" in data
    assert "ddl" in data


def test_list_pipelines():
    response = client.get("/pipelines/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_hypothesis_without_pipeline():
    response = client.post("/hypothesis/", json={"pipeline_id": 999})
    assert response.status_code == 404


def test_load_without_pipeline():
    response = client.post("/load-to-db/", json={"pipeline_id": 999})
    assert response.status_code == 404


