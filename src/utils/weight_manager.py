import os
import json

class WeightManager:
    @staticmethod
    def save_weight(weight_dict,filename):
        path = os.path.join("configs/weights",filename)
        os.makedirs(os.path.dirname(path),exist_ok=True)

        with open(path,'w') as f:
            json.dump(weight_dict,f,indent=4)

    @staticmethod
    def load_weight(filename):
        path = os.path.join("configs/weights",filename)
        with open(path,'r') as f:
            return json.load(f)