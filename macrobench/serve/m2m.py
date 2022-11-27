import ray
from ray import serve
import os
#from fastapi import FastAPI
from transformers import M2M100ForConditionalGeneration, M2M100Tokenizer
#app = FastAPI()
#ray.init(namespace='serve')
#serve.start(detached=True)
@serve.deployment(
    ray_actor_options={
        'num_cpus': 8
    }
)
class M2M:
        def __init__(self):
                self.model = M2M100ForConditionalGeneration.from_pretrained('facebook/m2m100_418M')
                self.tokenizer = M2M100Tokenizer.from_pretrained('facebook/m2m100_418M')
                self.src_lang = 'en'
                self.trg_lang = 'pt'
        def eval(self, txt: str):
                encoded_text = self.tokenizer(txt, return_tensors='pt', padding=True, truncation = True)
                generated_tokens = self.model.generate(**encoded_text,forced_bos_token_id=self.tokenizer.get_lang_id(self.trg_lang))
                return self.tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)
        def __call__(self, request):
                self.tokenizer.src_lang = self.src_lang
                txt = request.query_params['txt']
                ret = self.eval(txt)
                os.system('ray memory --stats-only')
                return self.eval(txt)
#M2M.deploy()

m2m_model = M2M.bind()
