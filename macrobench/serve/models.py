import torch

MODELS = ['Resnet18', 'Resnet50', 'Resnet101', 'BEiT', 'ConvNeXT', 'ViT384', 'Resnet18']

class ImgModel:
    def __init__(self, model_name, model):
        self.model = model
        self.model_name = model_name

    def eval(self, img):
        try:
            self._eval(img)
        except NotImplementedError:
            raise
        except Exception:
            print('Exception Occured at eval() ', self.model_name)

    def _eval(self, img):
        raise NotImplementedError

    def print_name(self):
        print(self.model_name)

class Resnet18(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-18")

        ImgModel.__init__(self, 'Resnet18', ResNetForImageClassification.from_pretrained("microsoft/resnet-18"))

    def eval(self, img):

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

class Resnet50(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-50")

        ImgModel.__init__(self, 'Resnet50', ResNetForImageClassification.from_pretrained("microsoft/resnet-50"))

    def eval(self, img):
        from datasets import load_dataset

        dataset = load_dataset("huggingface/cats-image")
        image = dataset["test"]["image"][0]
        inputs = self.feature_extractor(image, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

class Resnet101(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-101")

        ImgModel.__init__(self, 'Resnet101', ResNetForImageClassification.from_pretrained("microsoft/resnet-101"))

    def eval(self, img):
        from datasets import load_dataset

        dataset = load_dataset("huggingface/cats-image")
        image = dataset["test"]["image"][0]
        inputs = self.feature_extractor(image, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]


class BEiT(ImgModel):
    def __init__(self):
        from transformers import BeitFeatureExtractor, BeitForImageClassification
        model = BeitForImageClassification.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')
        self.feature_extractor = BeitFeatureExtractor.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')

        ImgModel.__init__(self, 'BEiT', model)

    def eval(self, img):
        from PIL import Image
        import requests

        url = 'http://images.cocodataset.org/val2017/000000039769.jpg'
        image = Image.open(requests.get(url, stream=True).raw)
        inputs = self.feature_extractor(images=image, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 21,841 ImageNet-22k classes
        predicted_class_idx = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_class_idx]

class ConvNeXT(ImgModel):
    def __init__(self):
        from transformers import ConvNextFeatureExtractor, ConvNextForImageClassification

        self.feature_extractor = ConvNextFeatureExtractor.from_pretrained("facebook/convnext-tiny-224")
        model = ConvNextForImageClassification.from_pretrained("facebook/convnext-tiny-224")

        ImgModel.__init__(self, 'ConvNeXT', model)

    def eval(self, img):
        from datasets import load_dataset

        dataset = load_dataset("huggingface/cats-image")
        image = dataset["test"]["image"][0]
        inputs = self.feature_extractor(image, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

class ViT384(ImgModel):
    def __init__(self):
        from transformers import ViTFeatureExtractor, ViTForImageClassification
        self.feature_extractor = ViTFeatureExtractor.from_pretrained('google/vit-base-patch16-384')
        model = ViTForImageClassification.from_pretrained('google/vit-base-patch16-384')
        ImgModel.__init__(self, 'ViT384', model)

    def eval(self, img):
        from PIL import Image
        import requests
        url = 'http://images.cocodataset.org/val2017/000000039769.jpg'
        image = Image.open(requests.get(url, stream=True).raw)
        inputs = self.feature_extractor(images=image, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 1000 ImageNet classes
        predicted_class_idx = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_class_idx]

'''
from transformers import SegformerFeatureExtractor, SegformerForImageClassification
from PIL import Image
import requests

url = "http://images.cocodataset.org/val2017/000000039769.jpg"
image = Image.open(requests.get(url, stream=True).raw)

feature_extractor = SegformerFeatureExtractor.from_pretrained("nvidia/mit-b0")
model = SegformerForImageClassification.from_pretrained("nvidia/mit-b0")

inputs = feature_extractor(images=image, return_tensors="pt")
outputs = model(**inputs)
logits = outputs.logits
# model predicts one of the 1000 ImageNet classes
predicted_class_idx = logits.argmax(-1).item()
print("Predicted class:", model.config.id2label[predicted_class_idx])
class MIT-B0(ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def eval(self, img):

class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def eval(self, img):

class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def eval(self, img):

class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def eval(self, img):

class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def eval(self, img):

'''
