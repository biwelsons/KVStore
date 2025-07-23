import com.google.gson.Gson;
import java.io.*;
import java.net.*;
import java.util.*;

/*
 * Projeto SD: ClienteX — Key-Value Store Distribuido
 * Funcionalidades conforme SECAO 4 do enunciado.
 */
public class Cliente {
    private static final Scanner scanner = new Scanner(System.in);
    private static final Gson gson = new Gson();
    private static final Random random = new Random();

    // Lista de servidores conhecidos pelo cliente (capturada via INIT, item 4a)
    private static List<InetSocketAddress> servidores = new ArrayList<>();

    // Mapa de timestamps por chave para garantir consistencia (item 4c)
    private static Map<String, Long> timestamps = new HashMap<>();

    // Porta do cliente para escutar respostas assincronas (item 4c)
    private static int portaCliente = 0;

    public static void main(String[] args) throws UnsupportedEncodingException {
        while (true) {

            // Menu interativo obrigatorio — 4a (apenas INIT, PUT, GET)
            System.out.println("\nEscolha uma opcao:");
            System.out.println("1 - INIT (inicializar cliente)");
            System.out.println("2 - PUT (inserir key-value)");
            System.out.println("3 - GET (consultar key)");
            System.out.print("Opcao: ");
            String opcao = scanner.nextLine().trim();

            switch (opcao) {
                case "1":
                    initCliente(); // SECAO 4a: Inicializacao
                    break;
                case "2":
                    put(); // SECAO 4b: PUT
                    break;
                case "3":
                    get(); // SECAO 4c: GET
                    break;
                default:
                    System.out.println("Opcao invalida.");
            }
        }
    }

    // SECAO 4a: Inicializacao do cliente
    private static void initCliente() {
        servidores.clear();

        System.out.print("Os servidores estao na mesma maquina (127.0.0.1)? (s/n): ");
        boolean mesmaMaquina = scanner.nextLine().trim().equalsIgnoreCase("s");
        for (int i = 1; i <= 3; i++) {
            String ip;
            int porta;
            if (mesmaMaquina) {
                ip = "127.0.0.1";
                System.out.print("Digite a porta do servidor " + i + ": ");
                porta = Integer.parseInt(scanner.nextLine().trim());
            } else {
                System.out.print("Digite o IP do servidor " + i + ": ");
                ip = scanner.nextLine().trim();
                System.out.print("Digite a porta do servidor " + i + ": ");
                porta = Integer.parseInt(scanner.nextLine().trim());
            }
            servidores.add(new InetSocketAddress(ip, porta));
        }

        // Captura porta para respostas assincronas (GET com WAIT_FOR_RESPONSE)
        System.out.print("Digite a porta para receber respostas assincronas (ex: 20000): ");
        portaCliente = Integer.parseInt(scanner.nextLine().trim());
        iniciarServidorDeRetorno(portaCliente);

        System.out.println("Cliente inicializado com " + servidores.size() + " servidores.");
    }

    // SECAO 4b: PUT
    private static void put() {
        if (servidores.isEmpty()) {
            System.out.println("Erro: Cliente nao inicializado (use opcao INIT primeiro).");
            return;
        }
        System.out.print("Digite a chave (key): ");
        String key = scanner.nextLine().trim();
        System.out.print("Digite o valor (value): ");
        String value = scanner.nextLine().trim();

        InetSocketAddress servidor = escolherServidorAleatorio();
        Mensagem msg = new Mensagem("PUT", key, value, 0, null, 0);

        Mensagem resposta = enviarMensagem(servidor, msg);
        if (resposta != null && "PUT_OK".equals(resposta.getTipo())) {
            // Atualiza timestamp local
            timestamps.put(key, resposta.getTimestamp());
            // Print conforme enunciado
            System.out.println("PUT_OK key: " + key +
                    " value " + value +
                    " timestamp " + resposta.getTimestamp() +
                    " realizada no servidor " + servidor.getAddress().getHostAddress() + ":" + servidor.getPort());
        } else {
            System.out.println("Falha no PUT.");
        }
    }

    // SECAO 4c: GET
    private static void get() {
        if (servidores.isEmpty()) {
            System.out.println("Erro: Cliente nao inicializado (use opcao INIT primeiro).");
            return;
        }
        System.out.print("Digite a chave (key) a ser buscada: ");
        String key = scanner.nextLine().trim();
        long tsCliente = timestamps.getOrDefault(key, 0L);

        InetSocketAddress servidor = escolherServidorAleatorio();

        try {
            String ipLocal = InetAddress.getLocalHost().getHostAddress();
            Mensagem msg = new Mensagem("GET", key, null, tsCliente, ipLocal, portaCliente);
            Mensagem resposta = enviarMensagem(servidor, msg);

            if (resposta != null) {
                if ("WAIT_FOR_RESPONSE".equals(resposta.getTipo())) {
                    // Print conforme enunciado
                    System.out.println("GET key: " + key + " WAIT_FOR_RESPONSE do servidor " +
                            servidor.getAddress().getHostAddress() + ":" + servidor.getPort());
                } else if ("GET_OK".equals(resposta.getTipo())) {
                    // Print conforme enunciado
                    System.out.println("GET key: " + key +
                            " value: " + resposta.getValue() +
                            " obtido do servidor " + servidor.getAddress().getHostAddress() + ":" + servidor.getPort() +
                            ", meu timestamp " + tsCliente + " e do servidor " + resposta.getTimestamp());
                    timestamps.put(key, resposta.getTimestamp());
                } else {
                    System.out.println("Resposta inesperada: " + resposta.getTipo());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Escolhe servidor aleatorio para qualquer operacao (4b, 4c)
    private static InetSocketAddress escolherServidorAleatorio() {
        return servidores.get(random.nextInt(servidores.size()));
    }

    // Envia mensagem via TCP, recebe resposta — obrigatoriedade de TCP
    private static Mensagem enviarMensagem(InetSocketAddress servidor, Mensagem mensagem) {
        try (Socket socket = new Socket(servidor.getAddress(), servidor.getPort());
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String json = gson.toJson(mensagem);
            out.println(json);

            String respostaJson = in.readLine();
            return gson.fromJson(respostaJson, Mensagem.class);

        } catch (IOException e) {
            System.out.println("Erro ao se conectar com o servidor " + servidor + ": " + e.getMessage());
            return null;
        }
    }

    // Listener assincrono para respostas GET_OK futuras (quando recebeu WAIT_FOR_RESPONSE)
    private static void iniciarServidorDeRetorno(int porta) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(porta)) {
                System.out.println("Servidor de retorno escutando na porta " + porta + "...");
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> {
                        try (
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                        ) {
                            String respostaJson = in.readLine();
                            Mensagem resposta = gson.fromJson(respostaJson, Mensagem.class);

                            // Print conforme enunciado
                            System.out.println("GET key: " + resposta.getKey() +
                                    " value: " + resposta.getValue() +
                                    " obtido do servidor [assincrono], meu timestamp " +
                                    timestamps.getOrDefault(resposta.getKey(), 0L) +
                                    " e do servidor " + resposta.getTimestamp());

                            timestamps.put(resposta.getKey(), resposta.getTimestamp());
                        } catch (IOException e) {
                            System.out.println("Erro ao receber resposta assincrona: " + e.getMessage());
                        }
                    }).start();
                }
            } catch (IOException e) {
                System.out.println("Erro no servidor de retorno: " + e.getMessage());
            }
        }).start();
    }
}
