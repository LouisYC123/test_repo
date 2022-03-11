import luigi, datetime
from yclib import reader, tester, merger, report, plugin

# project global variables
handler = reader.FileHandler()
project_config = handler.read_config("project.yaml")
luigi_config = luigi.configuration.get_config()
luigi_config.read("luigi.toml")
timemark = datetime.datetime.today().strftime("%Y%m%d-%H%M")

# read every single file and save as pkl
class ExtractRaw(luigi.Task):
    """
    Luigi task to read files in project data folder and save as a pickle file.
    ExtractRaw is called once for every file in project data folder (parallel process).

    Parameters
    ----------
    data_key : str
        identifies the dataset type (eg. 'payslips')  

    myfile : str
        file to be read (from Extract.file_list)

    Methods
    --------
    requires()
        Defines the task dependencies.

    output()
        Defines the task target output.

    run()
        Defines the task behaviour.
    
    Returns
    -------
    data : pickle file   
        pickle file for each file in project data folder

    """

    data_key = luigi.Parameter()
    myfile = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            "data/raw/"
            + self.data_key
            + "/"
            + reader.os.path.basename(self.myfile).split(".")[0]
            + ".pkl",
            format=luigi.format.Nop,
        )

    def run(self):
        data = handler.read_raw(self.myfile, project_config, self.data_key)
        with self.output().open("w") as f:
            handler.save(f, data, "pkl")
        del data


# run column test / concat files in one particular key in 'extract' yaml / change mixed data type to string
class Extract(luigi.Task):
    data_key = luigi.Parameter()
    file_list = luigi.ListParameter()

    def requires(self):
        return [ExtractRaw(self.data_key, files) for files in self.file_list]

    def output(self):
        return luigi.LocalTarget(
            "data/extract/" + str(self.data_key) + ".pkl", format=luigi.format.Nop
        )

    def run(self):
        data = handler.read_staging(str(reader.Path(self.input()[0].path).parent))
        data = reader.Importer(self.data_key, data, project_config).run()
        with self.output().open("w") as f:
            handler.save(f, data, "pkl")
        del data


class RunExtract(luigi.Task):
    def requires(self):
        file_list = reader.Classifier("sharedata", project_config).run()
        return [Extract(i, file_list[i]) for i in file_list]

    def output(self):
        return luigi.LocalTarget("data/flag/extract_SUCCESS.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write("SUCCESS")


class TransformPreException(luigi.Task):
    data_key = luigi.Parameter()
    myfile = luigi.Parameter()

    def requires(self):
        return RunExtract()

    def output(self):
        return luigi.LocalTarget(
            "data/transform/" + self.data_key + ".pkl", format=luigi.format.Nop
        )

    def run(self):
        data = handler.read_staging(self.myfile)
        for i in project_config["transform"][self.data_key]["settings"]:
            for key, val in i["plugin"].items():
                data = getattr(plugin, key)(data, val)
        data = tester.ExceptionTest(
            data, project_config["transform"][self.data_key], self.data_key
        ).run()
        data = reader.Formatter().rename_columns(
            data, project_config["transform"][self.data_key]
        )
        with self.output().open("w") as f:
            handler.save(f, data, "pkl")
        del data


class RunTransformPreException(luigi.Task):
    def requires(self):
        return [
            TransformPreException(
                key,
                "data/extract/" + project_config["transform"][key]["source"] + ".pkl",
            )
            for key in project_config["transform"]
            if project_config["transform"][key]["switch"]
        ]

    def output(self):
        return luigi.LocalTarget("data/flag/transform_SUCCESS.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write("Success")


class GenerateExceptionReports(luigi.Task):
    def requires(self):
        return RunTransformPreException()

    def output(self):
        return luigi.LocalTarget("data/exception.xlsx")

    def run(self):
        files = handler.read_exception("data/exception")
        with self.output().open("w") as f:
            report.ExceptionReport(f.path, files, project_config)


class TransfromPostException(luigi.Task):
    data_key = luigi.Parameter()
    config = luigi.DictParameter()

    def requires(self):
        return GenerateExceptionReports()

    def output(self):
        return luigi.LocalTarget(
            "data/transform_post_exception/" + self.data_key + ".pkl",
            format=luigi.format.Nop,
        )

    def run(self):
        """
        #exception_client_response.xlsx
        exceptions=reader.pd.read_excel('data/exception_client_response.xlsx',sheet_name=None)
        #read all .pkl files from data/transform to a dict
        data={key:reader.read_files('data/transform/'+key+'pkl') for key in project_config['transform_post_exception'] if project_config['transform_post_exception'][i]['switch']}
        #handle exceptions according to client responses (NaN value, dups Value, Wrong-date Value etc..)
        data=merger.ExceptionHandler(data,exceptions).identifier()
        # extra exceptions like: HDA, multiple employee code, ifa etc.. 
        data=merger.ExtraExceptionHandler(data,exceptions,project_config)
        """
        data = {
            key: handler.read_staging("data/transform/" + key + ".pkl")
            for key in self.config["source"]
        }
        for i in data:
            data[i] = reader.Formatter().change_type(
                data[i], project_config["transform"][i]
            )
        data = merger.PostExceptionTransform(data, self.config).run()
        with self.output().open("w") as f:
            handler.save(f, data, "pkl")


class RunTransfromPostException(luigi.Task):
    def requires(self):
        return [
            TransfromPostException(i, project_config["transform_post_exception"][i])
            for i in project_config["transform_post_exception"]
            if project_config["transform_post_exception"][i]["switch"]
        ]

    def output(self):
        return luigi.LocalTarget("data/flag/TransformPostException_SUCCESS.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write("Success")


class Load(luigi.Task):
    data_key = luigi.Parameter()
    config = luigi.DictParameter()

    def requires(self):
        return RunTransfromPostException()

    def output(self):
        return luigi.LocalTarget(
            "data/load/{"
            + self.config["Configuration"]["value"]
            + "}-"
            + timemark
            + "-"
            + self.data_key
            + ".csv",
            format=luigi.format.Nop,
        )

    def run(self):
        if self.data_key == "Configuration":
            data = report.SentinelReport().configuration(self.config["Configuration"])
        else:
            data = handler.read_staging(
                "data/transform_post_exception/"
                + self.config[self.data_key]["source"]
                + ".pkl"
            )
            data = report.SentinelReport().other_files(data, self.config[self.data_key])
        with self.output().open("w") as f:
            handler.save(f, data, "csv")


class RunLoad(luigi.Task):
    def requires(self):
        return [
            Load(i, project_config["Load"])
            for i in project_config["Load"]
            if project_config["Load"][i]["switch"]
        ]

    def output(self):
        return luigi.LocalTarget(
            "data/flag/sentinel_COMPLETE.txt", format=luigi.format.Nop
        )

    def run(self):
        flag = report.SentinelReport().final_archieve("data/load")
        with self.output().open("w") as f:
            f.write("Success")

