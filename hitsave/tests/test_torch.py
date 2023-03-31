import torch
import torch.nn
import torch.nn.functional as F

from hitsave.local.inspection import ExternalBinding, module_as_external_package


def test_runs():
    t = torch.Tensor([0, 1, 2, 3])


def test_torch_versions():
    e1 = module_as_external_package("torch")
    e2 = module_as_external_package("torch.nn")
    e3 = module_as_external_package("torch.nn.functional")
    assert isinstance(e1, ExternalBinding)
    assert isinstance(e2, ExternalBinding)
    assert isinstance(e3, ExternalBinding)
    assert e1.name == "torch"
    assert e2.name == "torch"
    assert e3.name == "torch"
    assert e1.version == e2.version == e3.version
