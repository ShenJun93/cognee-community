"""Real memory acceptance candidate, deliberately blocked pending isolation proof.

The opt-in selects execution only; it cannot bypass the worker's database path,
provider, fresh-process and network checks. Windows stays skipped under WDAC.
No global prune is used. Importing this module does not import Cognee.
"""

import hashlib
import importlib
import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_two_pass_remember_in_disposable_process(request, tmp_path):
    """Run only on an explicitly selected, approved non-Windows test runner."""
    if not request.config.getoption("--run-jenkins-memory"):
        pytest.skip("Real memory acceptance requires --run-jenkins-memory on an approved runner")
    if sys.platform == "win32":
        pytest.skip("Windows native execution remains blocked by the documented WDAC policy")
    from remember_worker import sandbox_environment

    worker = Path(__file__).with_name("remember_worker.py")
    root = tmp_path / "memory_runtime"
    result = subprocess.run(
        [sys.executable, "-I", "-B", str(worker), str(root)],
        cwd=tmp_path,
        env=sandbox_environment(root),
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    proof = json.loads((root / "acceptance_result.json").read_text(encoding="utf-8"))
    assert proof["status"] == "passed"
    assert proof["checks"] == [
        "documents",
        "vectors",
        "graph",
        "source_isolation",
        "dataset_isolation",
        "retained_ids",
        "repeat_sync",
    ]
    assert proof["network_attempts"] == 0
    assert proof["prune_calls"] == 0


def _stub_paid_models(monkeypatch):
    """Stub model calls only; remember, extraction and all stores remain real."""
    from cognee.infrastructure.llm.LLMGateway import LLMGateway
    from cognee.shared.data_models import Edge, KnowledgeGraph, Node, SummarizedContent

    async def structured(text_input, system_prompt, response_model, **kwargs):
        if isinstance(response_model, type) and issubclass(response_model, KnowledgeGraph):
            # Marker entities come only from supplied document content.
            names = [
                name
                for name in (
                    "Rotatedmarker",
                    "Deletedmarker",
                    "Newmarker",
                    "Othermarker",
                    "Stablemarker",
                )
                if name in text_input
            ]
            nodes = [
                Node(
                    id=name.lower(),
                    name=name,
                    type="Fixture",
                    description=name + " Jenkins fixture",
                )
                for name in names
            ]
            root = Node(
                id="jenkinsfixture",
                name="Jenkinsfixture",
                type="Fixture",
                description="Jenkins fixture root",
            )
            edges = [
                Edge(source=root.id, target=node.id, relationship="contains") for node in nodes
            ]
            return response_model(nodes=[root, *nodes], edges=edges)
        if isinstance(response_model, type) and issubclass(response_model, SummarizedContent):
            return response_model(summary=text_input[:200], description="Jenkins fixture")
        if response_model is str:
            return text_input
        raise AssertionError("Unstubbed model schema: acceptance must remain offline")

    async def embed(self, text):
        from cognee.infrastructure.databases.vector.embeddings.EmbeddingEngine import (
            EmbeddingResult,
        )

        size = int(self.get_vector_size())
        vectors = []
        for value in text:
            digest = hashlib.sha256(value.encode()).digest()
            vectors.append([digest[i % len(digest)] / 255 for i in range(size)])
        return EmbeddingResult(vectors=vectors, tokens=0)

    monkeypatch.setattr(LLMGateway, "acreate_structured_output", staticmethod(structured))
    for name in ("LiteLLMEmbeddingEngine", "OpenAICompatibleEmbeddingEngine"):
        module = importlib.import_module(
            "cognee.infrastructure.databases.vector.embeddings." + name
        )
        monkeypatch.setattr(getattr(module, name), "embed_text", embed)


async def exercise_memory(runtime):
    """Harness contract: cognee/user plus dataset-scoped real store readers.

    documents(dataset) returns {external_id: UUID} for all Jenkins documents;
    vector_texts(dataset) and graph_names(dataset) dump real stored payloads,
    not search results or mocked counts. Backend settings and network denial
    must be checked by the isolated fixture before yielding this runtime.
    """
    from test_jenkins import BASE_URL, FakeJenkins, _build

    from cognee_community_connector_jenkins import jenkins_source

    cognee = runtime.cognee
    rotated = _build(1, cause="Rotatedmarker")
    stable = _build(2, cause="Stablemarker")
    fake = FakeJenkins(
        {
            "api": {"builds": [rotated, stable]},
            "web": {"description": "Deletedmarker", "builds": []},
        }
    )
    other = FakeJenkins({"other": {"description": "Othermarker", "builds": []}})

    async def remember(dataset, client, scope, deleted=False):
        result = await cognee.remember(
            jenkins_source(
                base_url=BASE_URL,
                service=client,
                resource_name=scope,
                confirm_deleted=lambda name, number: (
                    deleted and (name == "web" or (name == "api" and number == 1))
                ),
            ),
            dataset_name=dataset,
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )
        assert result.status == "completed", result

    await remember("jenkins_a", fake, "primary")
    first = await runtime.documents("jenkins_a")
    assert set(first) == {"job:api", "build:api#1", "build:api#2", "job:web"}
    for reader in (runtime.vector_texts, runtime.graph_names):
        contents = "\n".join(await reader("jenkins_a")).lower()
        assert "rotatedmarker" in contents and "deletedmarker" in contents
    await remember("jenkins_b", fake, "primary")
    await remember("jenkins_a", other, "secondary")
    isolated_b = await runtime.documents("jenkins_b")
    with_other = await runtime.documents("jenkins_a")
    assert set(with_other) == {*first, "job:other"}
    assert set(isolated_b) == set(first)
    assert set(isolated_b.values()).isdisjoint(first.values())

    fake.jobs["api"]["builds"] = [stable, _build(3, cause="Newmarker")]
    del fake.jobs["web"]
    fake.folders = {"": ["api"]}
    await remember("jenkins_a", fake, "primary", deleted=True)
    second = await runtime.documents("jenkins_a")
    assert set(second) == {"job:api", "build:api#2", "build:api#3", "job:other"}
    assert second["job:api"] == first["job:api"]
    assert second["build:api#2"] == first["build:api#2"]
    assert first["build:api#1"] not in second.values()
    assert first["job:web"] not in second.values()
    for reader in (runtime.vector_texts, runtime.graph_names):
        contents = "\n".join(await reader("jenkins_a")).lower()
        assert "newmarker" in contents and "othermarker" in contents
        assert "rotatedmarker" not in contents and "deletedmarker" not in contents
    assert await runtime.documents("jenkins_b") == isolated_b
    for reader in (runtime.vector_texts, runtime.graph_names):
        contents = "\n".join(await reader("jenkins_b")).lower()
        assert "rotatedmarker" in contents and "deletedmarker" in contents
        assert "newmarker" not in contents and "othermarker" not in contents
    # Revisit both scopes: state/cursor collisions must not reintroduce old rows.
    await remember("jenkins_a", other, "secondary")
    await remember("jenkins_b", fake, "primary", deleted=True)
    assert await runtime.documents("jenkins_a") == second
    third = await runtime.documents("jenkins_b")
    assert set(third) == {"job:api", "build:api#2", "build:api#3"}
    assert set(third.values()).isdisjoint(second.values())
