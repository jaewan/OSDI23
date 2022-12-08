import torch
import ray

MODELS = ['Resnet18', 'Resnet50', 'Resnet101', 'BEiT', 'ConvNeXT', 'ViT384', 'Resnet18', 'MIT_B0']

class ImgModel:
    def __init__(self, model_name, model):
        self.model = model
        self.model_name = model_name

    def predict(self, img):
        try:
            self._predict(img)
        except NotImplementedError:
            raise
        except Exception:
            print('Exception Occured at predict() ', self.model_name)

    def _predict(self, img):
        raise NotImplementedError

    def print_name(self):
        print(self.model_name)

@ray.remote
class Resnet18(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-18")

        ImgModel.__init__(self, 'Resnet18', ResNetForImageClassification.from_pretrained("microsoft/resnet-18"))

    def predict(self, img):

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

@ray.remote
class Resnet50(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-50")

        ImgModel.__init__(self, 'Resnet50', ResNetForImageClassification.from_pretrained("microsoft/resnet-50"))

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

@ray.remote
class Resnet101(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-101")

        ImgModel.__init__(self, 'Resnet101', ResNetForImageClassification.from_pretrained("microsoft/resnet-101"))

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]


@ray.remote
class BEiT(ImgModel):
    def __init__(self):
        from transformers import BeitFeatureExtractor, BeitForImageClassification
        model = BeitForImageClassification.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')
        self.feature_extractor = BeitFeatureExtractor.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')

        ImgModel.__init__(self, 'BEiT', model)

    def predict(self, img):

        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 21,841 ImageNet-22k classes
        predicted_class_idx = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_class_idx]

@ray.remote
class ConvNeXT(ImgModel):
    def __init__(self):
        from transformers import ConvNextFeatureExtractor, ConvNextForImageClassification

        self.feature_extractor = ConvNextFeatureExtractor.from_pretrained("facebook/convnext-tiny-224")
        model = ConvNextForImageClassification.from_pretrained("facebook/convnext-tiny-224")

        ImgModel.__init__(self, 'ConvNeXT', model)

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

@ray.remote
class ViT384(ImgModel):
    def __init__(self):
        from transformers import ViTFeatureExtractor, ViTForImageClassification
        self.feature_extractor = ViTFeatureExtractor.from_pretrained('google/vit-base-patch16-384')
        model = ViTForImageClassification.from_pretrained('google/vit-base-patch16-384')
        ImgModel.__init__(self, 'ViT384', model)

    def predict(self, img):
        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 1000 ImageNet classes
        predicted_class_idx = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_class_idx]

@ray.remote
class MIT_B0(ImgModel):
    def __init__(self):
        from transformers import SegformerFeatureExtractor, SegformerForImageClassification
        self.feature_extractor = SegformerFeatureExtractor.from_pretrained("nvidia/mit-b0")
        model = SegformerForImageClassification.from_pretrained("nvidia/mit-b0")
        ImgModel.__init__(self, 'MIT_B0', model)

    def predict(self, img):
        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 1000 ImageNet classes
        predicted_class_idx = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_class_idx]
'''


@ray.remote
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img):

'''
