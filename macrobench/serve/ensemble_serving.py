import ray
import os
from models import *
import random
import numpy as np

params=0
def get_params():  
    import argparse 
    global params
    parser = argparse.ArgumentParser()
    parser.add_argument('--NUM_BATCHES', '-nb', type=int, default=1)
    parser.add_argument('--BATCH_SIZE', '-bs', type=int, default=2)
    parser.add_argument('--BATCH_INTERVAL', '-bi', type=float, default=0)
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../../data/ensemble_serve/dummy.csv")
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=8_000_000_000)
    parser.add_argument('--MAX_MODEL_RUN', '-m', type=int, default=10)
    args = parser.parse_args()
    params = vars(args) 

@ray.remote
def get_image(i):
    from PIL import Image
    #idx = random.randint(0, 9)
    idx = i%12
    if idx == 0:
        image = Image.open(r'/dev/shm/Double-Cat-Wallpaper.jpg')
    elif idx == 1:
        image = Image.open(r'/dev/shm/qVnC9UJ.jpg')
    elif idx == 2:
        image = Image.open(r'/dev/shm/wp11786853.jpg')
    elif idx == 3:
        image = Image.open(r'/dev/shm/wp11177599.jpg')
    elif idx == 4:
        image = Image.open(r'/dev/shm/wp3878871.jpg')
    elif idx == 5:
        image = Image.open(r'/dev/shm/wp11335732.jpg')
    elif idx == 6:
        image = Image.open(r'/dev/shm/uwp3008684.jpeg')
    elif idx == 7:
        image = Image.open(r'/dev/shm/uwp2993171.jpeg')
    elif idx == 8:
        image = Image.open(r'/dev/shm/104801ab.jpg')
    elif idx == 9:
        image = Image.open(r'/dev/shm/219648fg.jpg')
    elif idx == 10:
        image = Image.open(r'/dev/shm/400853mt.jpg')
    elif idx == 11:
        image = Image.open(r'/dev/shm/911093ab.jpg')
    return image

@ray.remote(num_returns=2)
def preprocess(img):
    import torchvision.transforms as transforms
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])
    '''
    preprocessing = transforms.Compose([
       transforms.RandomSizedCrop(224),
       transforms.RandomHorizontalFlip(),
       transforms.ToTensor(),
       normalize,
    ])
    '''
    preprocessing = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        normalize
    ])
    return preprocessing(img), np.zeros(50_000_000 // 8)
                                                                                                         
def initialize():
    import json
    get_params()

    # Initialize Ray to spill to local disk
    spill_dir = os.getenv('RAY_SPILL_DIR')
    if spill_dir:
        ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                    "params": {"directory_path": spill_dir}},)}, object_store_memory=params['OBJECT_STORE_SIZE'])
        #ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
        #                            "params": {"directory_path": spill_dir}},)})
        print("Ray spill dir set")
    else:
        ray.init(object_store_memory=params['OBJECT_STORE_SIZE'])
        print("Ray default init")


model_runtime = [
         0.007,
         0.007,
         0.011,
         0.018,
         0.006,
         0.059,
         0.016
]
def get_arbitrary_model():
    #return img_models[random.randint(0, num_models)]
    i = random.randint(0, len(img_models) - 1)
    return model_runtime[i], img_models[i]


@ray.remote(num_cpus=1)
def aggregator(img, seq):
    random.seed(seq)
    #random_numbers = []
    #for _ in range(1,params['MAX_MODEL_RUN']):
    #    random_numbers.append(random.randint(0,1))
    INITIAL_MODEL_BATCH = 3
    processed_img, original_image = preprocess.remote(img)

    calculation_time = 0
    prediction_ref = []
    predictions = {}
    for _ in range(INITIAL_MODEL_BATCH):
        t, m = get_arbitrary_model()
        #m = img_models[seq % len(img_models)]
        prediction_ref.append(m.simulate.remote(img, img, True, seq))
        seq = seq +1
    num_models_run = INITIAL_MODEL_BATCH

    for i in range(INITIAL_MODEL_BATCH):
        inference_time, pred = ray.get(prediction_ref[i])
        calculation_time += t
        predictions[pred] = predictions.get(pred,0) + 1
        if predictions[pred] >= ((num_models_run//2) + 1):
            return calculation_time, num_models_run

    vote = 0
    i = 0
    while vote < ((num_models_run//2) + 1) and num_models_run <= params['MAX_MODEL_RUN']:
        t, m = get_arbitrary_model()
        #m = img_models[seq % len(img_models)]
        #seq = seq +1
        run_original_image = random.randint(0, 1)# params['MAX_MODEL_RUN']//2)
        #run_original_image = random_numbers[i]
        i += 1
        pred = 0
        if run_original_image:
            inference_time, pred = ray.get(m.simulate.remote(img, img, False,seq))
            calculation_time += t
        else:
            inference_time, pred = ray.get(m.simulate.remote(processed_img, original_image, False, seq))
            calculation_time += t
        seq = seq +1

        num_models_run += 1
        predictions[pred] = predictions.get(pred,0) + 1
        vote = predictions[pred]

    return (calculation_time, num_models_run)


def batch_submitter():
    import time
    res = []
    for i in range(params['BATCH_SIZE']):
        #res.append(aggregator.remote(get_image.remote(i), i))
        res.append(aggregator.remote(get_image.remote(i), task_id))
        task_id += 1
    time.sleep(params['BATCH_INTERVAL'])
    return res

def store_results(runtime):
    import subprocess
    memory_stat = subprocess.run(['ray', 'memory', '--stats-only'], stdout=subprocess.PIPE)
    words = memory_stat.stdout.decode('utf-8').split()
    try:
        idx = words.index("Spilled")
        spilled = int(words[idx+1])
    except:
        spilled = 0
    try:
        idx = words.index("Restored")
        restored = int(words[idx+1])
    except:
        restored = 0

    import csv
    result_path = params['RESULT_PATH']
    result_csv = [runtime, params['NUM_BATCHES'],params['BATCH_SIZE'],params['BATCH_INTERVAL'], models_run, spilled, restored, params['OBJECT_STORE_SIZE']]
    print(result_csv)
    if 'dummy' not in result_path:
        with open(result_path, 'a', encoding='UTF-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(result_csv)

if __name__ == '__main__':
    from time import perf_counter

    initialize()

    # Load Images

    global img_models
    global num_models
    global task_id
    task_id = 0
    num_models = len(MODELS)
    img_models = []

    img_models.append(Resnet18.remote())
    img_models.append(Resnet50.remote())
    img_models.append(Resnet101.remote())
    img_models.append(BEiT.remote())
    img_models.append(ConvNeXT.remote())
    img_models.append(ViT384.remote())
    img_models.append(MIT_B0.remote())

    
    '''
    for i in range(12):
        img = get_image.remote(i)
        import time
        import os
        time.sleep(1)
        os.system('ray memory --stats-only')
        processed_img, original_image = preprocess.remote(img)
        time.sleep(1)
        os.system('ray memory --stats-only')
        res = []
        for model in img_models:
            res.append(model.predict.remote(img))

        print('\t\t Original Image')
        for r in res:
            print(ray.get(r))

        res = []
        for model in img_models:
            res.append(model.predict.remote(processed_img))

        print('\t\t Preprocessed Image')
        for r in res:
            print(ray.get(r))
   
    '''
    res = []
    start = perf_counter()
    for _ in range(params['NUM_BATCHES']):
        res.append(batch_submitter())

    models_run = 0
    three_run = 0
    total_calculation_time = 0
    for i in range(len(res)):
        for j in range(len(res[i])):
            calculation_time, mod_run = ray.get(res[i][j])
            total_calculation_time += calculation_time
            #print(j, perf_counter() - start)
            if mod_run == 3:
                three_run += 1
                #print("!", total_calculation_time, perf_counter() - start)
            models_run += mod_run
    end = perf_counter()

    print("***************", three_run, total_calculation_time)
    store_results(end-start)
