package br.com.kafka;

import br.com.kafka.ecommerce.service.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {


    private final Connection connection;

    public CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.bd";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (uuid varchar(200) primary key, " +
                    "email varchar (200))");
        } catch (SQLException e) {

        }

    }

    public static void main(String[] args) throws SQLException {

        var createUserService = new CreateUserService();

        try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse, Order.class, new HashMap<String, String>())) {
            service.run();
        }
    }


    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("::::_________________________________________________::::");
        System.out.println("Processsando new Order, checking for new User ");
        System.out.println("Chave " + record.key());
        System.out.println("Value " + record.value());

        var order = record.value();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        var insert = connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?, ? )");
        insert.setString(1, uuid);
        insert.setString(2, email);
        System.out.println("Usuário UUID "+ uuid +" adcionado!");
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users WHERE email = ? limit 1");
        exists.setString(1, email);
        var result = exists.executeQuery();
        return !result.next();
    }

}
