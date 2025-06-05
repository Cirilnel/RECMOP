"""
Model exported as python.
Name : INTERAZIONE PEB-NEB
Group :
With QGIS : 33601
"""

from qgis.core import QgsProcessing
from qgis.core import QgsProcessingAlgorithm
from qgis.core import QgsProcessingMultiStepFeedback
from qgis.core import QgsProcessingParameterVectorLayer
from qgis.core import QgsProcessingParameterFeatureSink
from qgis.core import QgsCoordinateReferenceSystem
import processing


class InterazionePebneb(QgsProcessingAlgorithm):

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterVectorLayer('input_negativo', 'INPUT NEGATIVO', types=[QgsProcessing.TypeVectorPolygon], defaultValue=None))
        self.addParameter(QgsProcessingParameterVectorLayer('input_positivo', 'INPUT POSITIVO', types=[QgsProcessing.TypeVectorPolygon], defaultValue=None))
        self.addParameter(QgsProcessingParameterFeatureSink('OutputNed2', 'OUTPUT NED2', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, supportsAppend=True, defaultValue='TEMPORARY_OUTPUT'))
        self.addParameter(QgsProcessingParameterFeatureSink('OutputPed2', 'OUTPUT PED 2', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, supportsAppend=True, defaultValue='TEMPORARY_OUTPUT'))
        self.addParameter(QgsProcessingParameterFeatureSink('NewNed', 'NEW NED', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, defaultValue='TEMPORARY_OUTPUT'))
        self.addParameter(QgsProcessingParameterFeatureSink('NewPed', 'NEW PED', type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, defaultValue='TEMPORARY_OUTPUT'))
        self.addParameter(QgsProcessingParameterFeatureSink('Aaaaaaa', 'aaaaaaa', optional=True, type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True, defaultValue=None))

    def processAlgorithm(self, parameters, context, model_feedback):
        # Use a multi-step feedback, so that individual child algorithm progress reports are adjusted for the
        # overall progress through the model
        feedback = QgsProcessingMultiStepFeedback(33, model_feedback)
        results = {}
        outputs = {}

        # Unisci attributi dal vettore più vicino - STEP 2
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELDS_TO_COPY': [''],
            'INPUT': parameters['input_positivo'],
            'INPUT_2': parameters['input_negativo'],
            'MAX_DISTANCE': None,
            'NEIGHBORS': 1,
            'PREFIX': None,
            'OUTPUT': parameters['Aaaaaaa']
        }
        outputs['UnisciAttributiDalVettorePiVicinoStep2'] = processing.run('native:joinbynearest', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['Aaaaaaa'] = outputs['UnisciAttributiDalVettorePiVicinoStep2']['OUTPUT']

        feedback.setCurrentStep(1)
        if feedback.isCanceled():
            return {}

        # Calcolo DELTA
        alg_params = {
            'FIELD_LENGTH': 20,
            'FIELD_NAME': 'DELTA',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': 'Surplus+Deficit',
            'INPUT': outputs['UnisciAttributiDalVettorePiVicinoStep2']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['CalcoloDelta'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(2)
        if feedback.isCanceled():
            return {}

        # Elimina campo_step3.0
        alg_params = {
            'COLUMN': ['n','distance','feature_x','feature_y','nearest_x','nearest_y'],
            'INPUT': outputs['CalcoloDelta']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['EliminaCampo_step30'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(3)
        if feedback.isCanceled():
            return {}

        # Group_stats_step4
        alg_params = {
            'CATEGORIES_FIELD_NAME': ['ID_N'],
            'INPUT': outputs['EliminaCampo_step30']['OUTPUT'],
            'VALUES_FIELD_NAME': 'DELTA',
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Group_stats_step4'] = processing.run('qgis:statisticsbycategories', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(4)
        if feedback.isCanceled():
            return {}

        # Unisci attributi secondo il valore del campo_step5.0
        alg_params = {
            'DISCARD_NONMATCHING': True,
            'FIELD': 'ID_N',
            'FIELDS_TO_COPY': ['max'],
            'FIELD_2': 'ID_N',
            'INPUT': outputs['EliminaCampo_step30']['OUTPUT'],
            'INPUT_2': outputs['Group_stats_step4']['OUTPUT'],
            'METHOD': 1,  # Prendi solamente gli attributi del primo elemento corrispondente (uno-a-uno)
            'PREFIX': None,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['UnisciAttributiSecondoIlValoreDelCampo_step50'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(5)
        if feedback.isCanceled():
            return {}

        # Calcolatore di campi_STEP5.1
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'delta2',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': '"max"',
            'INPUT': outputs['UnisciAttributiSecondoIlValoreDelCampo_step50']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['CalcolatoreDiCampi_step51'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(6)
        if feedback.isCanceled():
            return {}

        # Estrai tramite espressione_step5.2
        alg_params = {
            'EXPRESSION': ' "DELTA"  =  "delta2" ',
            'INPUT': outputs['CalcolatoreDiCampi_step51']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['EstraiTramiteEspressione_step52'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(7)
        if feedback.isCanceled():
            return {}

        # Calcolatore di campi_agr_Step6
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'Agr',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 1,  # Intero (32 bit)
            'FORMULA': '@id ',
            'INPUT': outputs['EstraiTramiteEspressione_step52']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['CalcolatoreDiCampi_agr_step6'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(8)
        if feedback.isCanceled():
            return {}

        # Creazione_Pasned2_step9.2
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'ID_N',
            'FIELDS_TO_COPY': ['ID_P','DELTA','Agr'],
            'FIELD_2': 'ID_N',
            'INPUT': parameters['input_negativo'],
            'INPUT_2': outputs['CalcolatoreDiCampi_agr_step6']['OUTPUT'],
            'METHOD': 1,  # Prendi solamente gli attributi del primo elemento corrispondente (uno-a-uno)
            'PREFIX': None,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Creazione_pasned2_step92'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(9)
        if feedback.isCanceled():
            return {}

        # NEW_ned2
        # I NED CHE NON HANNO PARTECIPATO ALL'AGGREGAZIONE
        alg_params = {
            'EXPRESSION': '"Agr" is null',
            'INPUT': outputs['Creazione_pasned2_step92']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['New_ned2'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(10)
        if feedback.isCanceled():
            return {}

        # Creazione_Pasped_step7.1
        alg_params = {
            'DISCARD_NONMATCHING': True,
            'FIELD': 'ID_P',
            'FIELDS_TO_COPY': ['ID_N','DELTA','Agr'],
            'FIELD_2': 'ID_P',
            'INPUT': parameters['input_positivo'],
            'INPUT_2': outputs['CalcolatoreDiCampi_agr_step6']['OUTPUT'],
            'METHOD': 1,  # Prendi solamente gli attributi del primo elemento corrispondente (uno-a-uno)
            'PREFIX': None,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Creazione_pasped_step71'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(11)
        if feedback.isCanceled():
            return {}

        # Creazione_Pasned_step7.2
        alg_params = {
            'DISCARD_NONMATCHING': True,
            'FIELD': 'ID_N',
            'FIELDS_TO_COPY': ['ID_P','DELTA','Agr'],
            'FIELD_2': 'ID_N',
            'INPUT': parameters['input_negativo'],
            'INPUT_2': outputs['CalcolatoreDiCampi_agr_step6']['OUTPUT'],
            'METHOD': 1,  # Prendi solamente gli attributi del primo elemento corrispondente (uno-a-uno)
            'PREFIX': None,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Creazione_pasned_step72'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(12)
        if feedback.isCanceled():
            return {}

        # Creazione_Pasped2_step9.1
        alg_params = {
            'DISCARD_NONMATCHING': False,
            'FIELD': 'ID_P',
            'FIELDS_TO_COPY': ['ID_N','DELTA','Agr'],
            'FIELD_2': 'ID_P',
            'INPUT': parameters['input_positivo'],
            'INPUT_2': outputs['CalcolatoreDiCampi_agr_step6']['OUTPUT'],
            'METHOD': 1,  # Prendi solamente gli attributi del primo elemento corrispondente (uno-a-uno)
            'PREFIX': None,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Creazione_pasped2_step91'] = processing.run('native:joinattributestable', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(13)
        if feedback.isCanceled():
            return {}

        # Fondi vettori_step7.3
        alg_params = {
            'CRS': QgsCoordinateReferenceSystem('EPSG:32633'),
            'LAYERS': [outputs['Creazione_pasped_step71']['OUTPUT'],outputs['Creazione_pasned_step72']['OUTPUT']],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['FondiVettori_step73'] = processing.run('native:mergevectorlayers', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(14)
        if feedback.isCanceled():
            return {}

        # Dissolvi_step7.4
        alg_params = {
            'FIELD': ['Agr'],
            'INPUT': outputs['FondiVettori_step73']['OUTPUT'],
            'SEPARATE_DISJOINT': False,
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Dissolvi_step74'] = processing.run('native:dissolve', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(15)
        if feedback.isCanceled():
            return {}

        # NEW_ped2
        # I PED CHE NON HANNO PARTECIPATO ALL'AGGREGAZIONE
        alg_params = {
            'EXPRESSION': '"Agr" is null',
            'INPUT': outputs['Creazione_pasped2_step91']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['New_ped2'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(16)
        if feedback.isCanceled():
            return {}

        # Elimina campo_step7.5
        alg_params = {
            'COLUMN': ['layer','path'],
            'INPUT': outputs['Dissolvi_step74']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['EliminaCampo_step75'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(17)
        if feedback.isCanceled():
            return {}

        # Creazione_newned_step8.1
        alg_params = {
            'EXPRESSION': '"Delta"<0',
            'INPUT': outputs['EliminaCampo_step75']['OUTPUT'],
            'OUTPUT': parameters['NewNed']
        }
        outputs['Creazione_newned_step81'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['NewNed'] = outputs['Creazione_newned_step81']['OUTPUT']

        feedback.setCurrentStep(18)
        if feedback.isCanceled():
            return {}

        # NED2
        alg_params = {
            'CRS': QgsCoordinateReferenceSystem('EPSG:32633'),
            'LAYERS': [outputs['Creazione_newned_step81']['OUTPUT'],outputs['New_ned2']['OUTPUT']],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Ned2'] = processing.run('native:mergevectorlayers', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(19)
        if feedback.isCanceled():
            return {}

        # Creazione_newped_step8.2
        alg_params = {
            'EXPRESSION': '"Delta">=0',
            'INPUT': outputs['EliminaCampo_step75']['OUTPUT'],
            'OUTPUT': parameters['NewPed']
        }
        outputs['Creazione_newped_step82'] = processing.run('native:extractbyexpression', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['NewPed'] = outputs['Creazione_newped_step82']['OUTPUT']

        feedback.setCurrentStep(20)
        if feedback.isCanceled():
            return {}

        # Aggiorna ned step finale 1
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'Deficit2',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': 'if("Deficit" is null, "DELTA","Deficit")',
            'INPUT': outputs['Ned2']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaNedStepFinale1'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(21)
        if feedback.isCanceled():
            return {}

        # PED2
        alg_params = {
            'CRS': QgsCoordinateReferenceSystem('EPSG:32633'),
            'LAYERS': [outputs['Creazione_newped_step82']['OUTPUT'],outputs['New_ped2']['OUTPUT']],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['Ped2'] = processing.run('native:mergevectorlayers', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(22)
        if feedback.isCanceled():
            return {}

        # Aggiorna id ned step finale 2
        alg_params = {
            'FIELD_LENGTH': 100,
            'FIELD_NAME': 'ID_N2',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 2,  # Testo (stringa)
            'FORMULA': '"ID_N"+\',\'+"ID_P"',
            'INPUT': outputs['AggiornaNedStepFinale1']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaIdNedStepFinale2'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(23)
        if feedback.isCanceled():
            return {}

        # Aggiorna ped step finale 1
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'Surplus2',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': 'if("DELTA" is null, "Surplus","DELTA")',
            'INPUT': outputs['Ped2']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaPedStepFinale1'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(24)
        if feedback.isCanceled():
            return {}

        # Elimina campo ned step finale 3
        alg_params = {
            'COLUMN': ['Deficit','Surplus','ID_P','ID_N','DELTA','Agr','layer','path'],
            'INPUT': outputs['AggiornaIdNedStepFinale2']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['EliminaCampoNedStepFinale3'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(25)
        if feedback.isCanceled():
            return {}

        # Aggiorna id ped step finale 2
        alg_params = {
            'FIELD_LENGTH': 100,
            'FIELD_NAME': 'ID_P2',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 2,  # Testo (stringa)
            'FORMULA': '"ID_P"+\',\'+"ID_N"',
            'INPUT': outputs['AggiornaPedStepFinale1']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaIdPedStepFinale2'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(26)
        if feedback.isCanceled():
            return {}

        # Elimina campo ped step finale 3
        alg_params = {
            'COLUMN': ['Deficit','Surplus','ID_P','ID_N','DELTA','Agr','layer','path'],
            'INPUT': outputs['AggiornaIdPedStepFinale2']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['EliminaCampoPedStepFinale3'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(27)
        if feedback.isCanceled():
            return {}

        # Aggiorna ped step finale 4
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'Surplus',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': '"Surplus2"',
            'INPUT': outputs['EliminaCampoPedStepFinale3']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaPedStepFinale4'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(28)
        if feedback.isCanceled():
            return {}

        # Aggiorna ned step finale 4
        alg_params = {
            'FIELD_LENGTH': 10,
            'FIELD_NAME': 'Deficit',
            'FIELD_PRECISION': 2,
            'FIELD_TYPE': 0,  # Decimale (doppia precisione)
            'FORMULA': '"Deficit2"',
            'INPUT': outputs['EliminaCampoNedStepFinale3']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaNedStepFinale4'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(29)
        if feedback.isCanceled():
            return {}

        # Aggiorna ned step finale 5
        alg_params = {
            'FIELD_LENGTH': 100,
            'FIELD_NAME': 'ID_N',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 2,  # Testo (stringa)
            'FORMULA': '"ID_N2"',
            'INPUT': outputs['AggiornaNedStepFinale4']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaNedStepFinale5'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(30)
        if feedback.isCanceled():
            return {}

        # Aggiorna ped step finale 5
        alg_params = {
            'FIELD_LENGTH': 100,
            'FIELD_NAME': 'ID_P',
            'FIELD_PRECISION': 0,
            'FIELD_TYPE': 2,  # Testo (stringa)
            'FORMULA': '"ID_P2"',
            'INPUT': outputs['AggiornaPedStepFinale4']['OUTPUT'],
            'OUTPUT': QgsProcessing.TEMPORARY_OUTPUT
        }
        outputs['AggiornaPedStepFinale5'] = processing.run('native:fieldcalculator', alg_params, context=context, feedback=feedback, is_child_algorithm=True)

        feedback.setCurrentStep(31)
        if feedback.isCanceled():
            return {}

        # Elimina campo ned step finale 6
        alg_params = {
            'COLUMN': ['Surplus2','ID_P2'],
            'INPUT': outputs['AggiornaPedStepFinale5']['OUTPUT'],
            'OUTPUT': parameters['OutputPed2']
        }
        outputs['EliminaCampoNedStepFinale6'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['OutputPed2'] = outputs['EliminaCampoNedStepFinale6']['OUTPUT']

        feedback.setCurrentStep(32)
        if feedback.isCanceled():
            return {}

        # Elimina campo ned step finale 6
        alg_params = {
            'COLUMN': ['Deficit2','ID_N2'],
            'INPUT': outputs['AggiornaNedStepFinale5']['OUTPUT'],
            'OUTPUT': parameters['OutputNed2']
        }
        outputs['EliminaCampoNedStepFinale6'] = processing.run('native:deletecolumn', alg_params, context=context, feedback=feedback, is_child_algorithm=True)
        results['OutputNed2'] = outputs['EliminaCampoNedStepFinale6']['OUTPUT']
        return results

    def name(self):
        return 'INTERAZIONE PEB-NEB'

    def displayName(self):
        return 'INTERAZIONE PEB-NEB'

    def group(self):
        return ''

    def groupId(self):
        return ''

    def createInstance(self):
        return InterazionePebneb()
