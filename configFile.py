import yaml


class configFile:
    def __init__(self, filename):
        with open(filename, 'r') as f:
            cfgFile = yaml.safe_load(f)
            self.accessKey = cfgFile['aws']['accessKey']
            self.secretKey = cfgFile['aws']['secretKey']
            self.availabilityZone = cfgFile['aws']['availabilityZone']
            self.bucketName = cfgFile['aws']['bucketName']
