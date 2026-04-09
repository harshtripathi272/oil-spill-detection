from __future__ import annotations

import segmentation_models_pytorch as smp
import torch.nn as nn


def build_unet(encoder_name: str = "efficientnet-b0", in_channels: int = 3, classes: int = 1) -> nn.Module:
    return smp.Unet(
        encoder_name=encoder_name,
        encoder_weights="imagenet",
        in_channels=in_channels,
        classes=classes,
        activation=None,
    )
