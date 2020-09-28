import apis.processor as processor
import apis.skredvarsel.skredvarsel as sv


class SkrevarselProcessor(processor.Processor):
    def process(self):
        unprocessed = sv.get_data()
        pass
