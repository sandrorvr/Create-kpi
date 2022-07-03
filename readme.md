__*Descricao*__:<br>
    Estrutura de testes e deploy de kpis utilizando pyspark

__*Modo de uso*__:<br>
    1. Crie as classes filhas de KPI
    2. Crie uma classe filha de phase.Phase
    3. Sobrescreva os metodos getDataFrame(), preprocess(), runKpis()



__*Classes*__:<br>
    *KPI > classe abstrata para criar kpis
    *Phase >  Classe abstrata desenvolver as interacões entre os kpis
    *SaveDF > Classe concreta usada para savar um SparkDataFrame
    *StepsLoad > Classe concretar usada para definir o numero de interacões afim de concluir um grande volume de dados


__*KPI*__:<br>
    ## KPI(df, titleKPI, columnsId=None, columnsUsed=None)
    
        df : [SparkDataFrame] -> DataFrame base para a criacão do kpi

        titleKPI : [str] -> titulo do kpi

        columnsId : [ str | list[str] | None ] -> Coluna(s) contida(s) em self.df usada(s) para criar um coluna com valores unicos. O nome dessa coluna será a concatenacao da(s) coluna(s), separadas por '_'. Default=None

        columnsUsed : [ list[str] ] -> Todas as colunas usadas na criacão do kpi. Default=None

        @abstractMethod
        configKPI() -> Metodo que deve ser sobrescrito afim de definir o comportamento que o kpi deve possuir. Todo comportamento deve ser escrito sobre self.df
    
        getDF() : [ SparkDataFrame ] -> Retorna self.df corrente

        getLabels() : [ list[str] ] -> Retorna uma lista de labels usados na classificacão do kpi

        setLabels(lb : str) -> Configura as categorias usadas na saída do kpi

        run() : [ class(KPI) ] -> Cria o kpi e retorna uma instância de KPI 


    obs: Se columnsId=None irá retornar um SparkDataFrame como duas colunas (self.columnsUsed, self.titleKPI)

    return SparkDataFrame

__*Phase*__:<br>
    ## Phase(phaseTitle, mode, formatType='parquet', start=None, end=None, path=None, db='test', collection='test')

        phaseTitle : [ str ] -> titulo da fase

        mode : [ str ] -> Modo de escrita em disco 
                            append: Anexar o conteúdo do dataframe aos dados ou tabelas existentes, 
                            overwrite: Substituir os dados existentes com o conteúdo do dataframe, 
                            ignore: Ignorar a operação de gravação atual se os dados / tabela já existirem sem nenhum erro
        
        formatType : [ str | None ] -> parquet | csv | json. Default='parquet' 

        start : [ str | datetime | None ] -> Data de início dos dados que devem ser processados. Se for None, start será a menor data encontrada no SparkDataFrame de referência

        end : [ str | datetime | None ] -> Data de término dos dados que devem ser processados. Se for None, start será a maior data encontrada no SparkDataFrame de referência

        path : [ str ] = Caminho no qual o SparkDataFrame será escrito

        db : [ str ] = Nome do dataBase alocado em um SGBD. Default='test'

        collection : [ str ] = Nome da collection alocada em dataBase. Default='test'

        setMode(mode : [ str ]) -> Configura o modo como a fase irá funcionar. [ load | test ]

        setConfigSpark(configDict : [dict[str:value]]) -> insere variáveis de configuracões externas a classe através de um único dicionário [dict]. Necessariamente deve ser passado o par chave:valor ['spark': SparkSession] e ['columnDate': 'nome_da_coluna_de_data_usada']

        getConfigSpark() : [ dict[str:value] ] -> Retorna dicionário contendo as variáveis externas a classe

        @abstractMethod
        getDataFrame() : [ SparkDataFrame ] -> Essa classe deve ser sobrescrita afim de definir como o SaparkDataFrame base deve ser inserido. É possivel usar Phase.transformInDataFramePandas(df_pandas) para transformar o pandas.DataFrame em um SparkDataFrame

        @abstractMethod
        preprocess(df : SparkDataFrame) : [ SparkDataFrame ] -> Essa classe deve ser sobrescrita afim de definir as etapas de pré-processamento sobre df.

        @abstractMethod
        runKpis(df : SparkDataFrame) : [ SparkDataFrame ] -> Essa classe deve ser sobrescrita afim de definir a interacão entre os kpis
