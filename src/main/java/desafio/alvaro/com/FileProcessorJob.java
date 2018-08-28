package desafio.alvaro.com;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.desafio.model.Customer;
import com.desafio.model.Item;
import com.desafio.model.Sale;
import com.desafio.model.Salesman;

public class FileProcessorJob implements Job {

	private List<Customer> listCustumer = new ArrayList<Customer>();
	private List<Salesman> listSalesman = new ArrayList<Salesman>();
	private List<Sale> listSale = new ArrayList<Sale>();
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		try {
			lerArqivos();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	
	private void lerArqivos() throws IOException, URISyntaxException{
		
		FileFilter filter = new FileFilter() {
		    public boolean accept(File file) {
		        return file.getName().endsWith(".dat");
		    }
		};

		
		//Busca os arquivos no Diretório
		System.out.println(App.class.getResource("/").getPath());
		File dir = new File(App.class.getResource("/").getPath() + "/data/in");
		
		File[] files = dir.listFiles(filter);
		
		//Processa cada arquivo encontrado
		Arrays.asList(files).forEach(file->{
			System.out.println(">>>>>>> Lendo arquivo: " + file.getName());
			if(file.exists()){
				Path caminho = Paths.get(file.getPath());
				Stream<String> rows = null;
				try {
					rows = Files.lines(caminho, StandardCharsets.ISO_8859_1);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				
				rows.forEach(row -> {
					try {
						String[] valores = row.split("ç");
						if("001".equals(valores[0])){
							listSalesman.add(factorySalesman(valores));
						}else if("002".equals(valores[0])){
							listCustumer.add(factoryCustomer(valores));
						}else if("003".equals(valores[0])){
							listSale.add(factorySale(valores));
						}else{
							throw new Exception("Row type not identified!");
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
		});
		processData();
		listClear();
	}
	
	private void listClear() {
		this.listCustumer.clear();
		this.listSale.clear();
		this.listSalesman.clear();		
	}


	/**
	 * Método que processa os dados
	 * @throws IOException
	 */
	private void processData() throws IOException{
		
		StringBuilder sb = new StringBuilder();
		sb.append("Quantidade de clientes no arquivo de entrada: " + listCustumer.size() + "\n");
		sb.append("Quantidade de vendedor no arquivo de entrada: " + listSalesman.size() + "\n");

		List<Sale> newListSale = listSale.stream().sorted((sale1, sale2)->
		sale1.getTotalSale().compareTo(sale2.getTotalSale())).collect(Collectors.toList());
		
		sb.append("A venda mais cara foi: id " + newListSale.get(newListSale.size()-1).getId() + " no valor de R$: "+ newListSale.get(newListSale.size()-1).getTotalSale() +"\n");
		
		Map<String, Double> values =  newListSale.stream().collect(Collectors.groupingBy(Sale::getSalesmanName, Collectors.summingDouble(Sale::getTotalSale)))
			.entrySet().stream().sorted(Map.Entry.comparingByValue())
			.collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (e1, e2) -> e2, LinkedHashMap::new));
		
		 Map.Entry<String,Double> entry = values.entrySet().iterator().next();
		 String key= entry.getKey();
		 
		 sb.append("O Pior Vendedor já " + key);
		 
		 saveFile(sb.toString());
		 
	}
	
	/**
	 * Método que salva o arquivo
	 * @param text
	 * @throws IOException
	 */
	private void saveFile(String text) throws IOException{
		System.out.println(text);
		File dir = new File(App.class.getResource("/").getPath() + "/data/out");
		
		String fileName = "flat_file_name.done.dat";
		FileWriter arq = new FileWriter(dir.getPath()+"/"+fileName);
		PrintWriter writer = new PrintWriter(arq);
		writer.print(text);
		arq.close();
	}
	
	/**
	 * Método que monta a classe de Vendedor
	 * @param row
	 * @return
	 * @throws Exception
	 */
	private Salesman factorySalesman(String[] row) throws Exception{
		if(!"001".equals(row[0])){
			throw new Exception("Wrong type row!");
		}
		
		Salesman salesman = new Salesman(row[1], row[2], row[3]);
		
		System.out.println(salesman.toString());
		return salesman;
		
	}

	/**
	 * Método que monta a classe de Cliente
	 * @param row
	 * @return
	 * @throws Exception
	 */
	private Customer factoryCustomer(String[] row) throws Exception{
		
		if(!"002".equals(row[0])){
			throw new Exception("Wrong type row!");
		}
		
		Customer customer = new Customer(row[1], row[2], row[3]);
		
		System.out.println(customer.toString());
		
		return customer;
	}

	
	/**
	 * Método que monta a classe de vendas
	 * @param row
	 * @return
	 * @throws Exception
	 */
	private Sale factorySale(String[] row) throws Exception{
	
		if(!"003".equals(row[0])){
			throw new Exception("Wrong type row!");
		}
		
		Sale sale = new Sale(Integer.parseInt(row[1]), row[3]);
		
		String[] itemsString = row[2].replace("[", "").replace("]", "").split(",");
		
		Arrays.asList(itemsString).forEach(item ->{
			String[] detailItem = item.split("-");
			Item newItem = new Item(Long.parseLong(detailItem[0]), Integer.parseInt(detailItem[1]), new BigDecimal(detailItem[2]));
			sale.getItems().add(newItem);
		});
		
		System.out.println(sale.toString());
		
		return sale;
	}
	
}
