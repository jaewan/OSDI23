import torch
import ray
import time
import random

SIMULATE=False

MODELS = ['Resnet18', 'Resnet50', 'Resnet101', 'BEiT', 'ConvNeXT', 'ViT384', 'MIT_B0']
model_runtime = {
        'Resnet18' : 0.007,
        'Resnet50' : 0.007,
        'Resnet101' : 0.011,
        'BEiT' : 0.018,
        'ConvNeXT' : 0.006,
        'ViT384' : 0.059,
        'MIT_B0' : 0.016
}
'''
model_runtime = {
        'Resnet18' : 0,
        'Resnet50' : 0,
        'Resnet101' : 0,
        'BEiT' : 0,
        'ConvNeXT' : 0,
        'ViT384' : 0,
        'MIT_B0' : 0
}
'''
def simulation(first, model_name):
    time.sleep(model_runtime[model_name])
    if first:
        idx = random.randint(0, 5)
        if idx == 0:
            return 'dog'
        elif idx == 1:
            return 'tiger'
        elif idx == 2:
            return 'horse'
        elif idx == 3:
            return 'bat'
        elif idx == 4:
            return 'elephant'
        else:
            return 'cat'
    prob = 5
    idx = random.randint(0, prob)
    if idx < prob:
        return 'cat'
    else:
        return 'dog'

class ImgModel:
    def __init__(self, model_name, model):
        self.model = model
        self.model_name = model_name
        random.seed(MODELS.index(model_name))

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

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class Resnet18(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-18")

        ImgModel.__init__(self, 'Resnet18', ResNetForImageClassification.from_pretrained("microsoft/resnet-18"))
        random.seed(0)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        return self.model.config.id2label[predicted_label]

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class Resnet50(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-50")

        ImgModel.__init__(self, 'Resnet50', ResNetForImageClassification.from_pretrained("microsoft/resnet-50"))
        random.seed(1)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        ret =  self.model.config.id2label[predicted_label]
        return ret

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class Resnet101(ImgModel):
    def __init__(self):
        from transformers import AutoFeatureExtractor, ResNetForImageClassification

        self.feature_extractor = AutoFeatureExtractor.from_pretrained("microsoft/resnet-101")

        ImgModel.__init__(self, 'Resnet101', ResNetForImageClassification.from_pretrained("microsoft/resnet-101"))
        random.seed(2)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        ret =  self.model.config.id2label[predicted_label]
        return ret


#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class BEiT(ImgModel):
    def __init__(self):
        from transformers import BeitFeatureExtractor, BeitForImageClassification
        model = BeitForImageClassification.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')
        self.feature_extractor = BeitFeatureExtractor.from_pretrained('microsoft/beit-base-patch16-224-pt22k-ft22k')

        ImgModel.__init__(self, 'BEiT', model)
        random.seed(3)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):

        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 21,841 ImageNet-22k classes
        predicted_class_idx = logits.argmax(-1).item()
        ret =  self.model.config.id2label[predicted_class_idx]
        return ret

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class ConvNeXT(ImgModel):
    def __init__(self):
        from transformers import ConvNextFeatureExtractor, ConvNextForImageClassification

        self.feature_extractor = ConvNextFeatureExtractor.from_pretrained("facebook/convnext-tiny-224")
        model = ConvNextForImageClassification.from_pretrained("facebook/convnext-tiny-224")

        ImgModel.__init__(self, 'ConvNeXT', model)
        random.seed(4)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):
        from datasets import load_dataset

        inputs = self.feature_extractor(img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model(**inputs).logits

        # model predicts one of the 1000 ImageNet classes
        predicted_label = logits.argmax(-1).item()
        ret =  self.model.config.id2label[predicted_label]
        return ret

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class ViT384(ImgModel):
    def __init__(self):
        from transformers import ViTFeatureExtractor, ViTForImageClassification
        self.feature_extractor = ViTFeatureExtractor.from_pretrained('google/vit-base-patch16-384')
        model = ViTForImageClassification.from_pretrained('google/vit-base-patch16-384')
        ImgModel.__init__(self, 'ViT384', model)
        random.seed(5)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):
        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 1000 ImageNet classes
        predicted_class_idx = logits.argmax(-1).item()
        ret =  self.model.config.id2label[predicted_class_idx]
        return ret

#@ray.remote(num_cpus=0, num_gpus=0.14)
@ray.remote(num_cpus=1)
class MIT_B0(ImgModel):
    def __init__(self):
        from transformers import SegformerFeatureExtractor, SegformerForImageClassification
        self.feature_extractor = SegformerFeatureExtractor.from_pretrained("nvidia/mit-b0")
        model = SegformerForImageClassification.from_pretrained("nvidia/mit-b0")
        ImgModel.__init__(self, 'MIT_B0', model)
        random.seed(6)

    def simulate(self, img, original_image, first):
        if SIMULATE:
            return simulation(first, self.model_name)
        return self.predict(img)

    def predict(self, img):
        inputs = self.feature_extractor(images=img, return_tensors="pt")
        outputs = self.model(**inputs)
        logits = outputs.logits
        # model predicts one of the 1000 ImageNet classes
        predicted_class_idx = logits.argmax(-1).item()
        ret = self.model.config.id2label[predicted_class_idx]
        return ret
'''


@ray.remote
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img, original_image):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img, original_image):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img, original_image):

@ray.remote(num_cpus=1)
class (ImgModel):
    def __init__(self):

        ImgModel.__init__(self, '', )

    def predict(self, img, original_image):

'''
