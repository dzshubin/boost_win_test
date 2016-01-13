#include <iostream>
#include <functional>
#include <memory>
#include <string>
#include <array>
#include <vector>
#include <boost/asio/io_service.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/basic_socket_acceptor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/basic_endpoint.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <mysql_connection.h>
#include <mysql_driver.h>


#include <boost/serialization/access.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>


#include <cppconn/connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>

using namespace std;
using namespace sql;
using namespace boost::asio;



class Session : public std::enable_shared_from_this<Session>
{
public:
    Session (ip::tcp::socket socket_)
      :m_socket(std::move(socket_)), m_size(0)
    {
        m_vecData.reserve(20);
    }

    void start ()
    {
        do_read();
    }


public:
    enum class Size  { MAX_SIZE = 2048 };

    using err_code = boost::system::error_code;

private:
    void do_read ()
    {
        //std::cout << "size is: " << m_size << endl;
        auto self = shared_from_this();
        boost::asio::async_read (m_socket, buffer(data),
            [this, self] (const err_code& ec, size_t len)
            {
                if (!ec)
                {
                        m_size = std::stoi(data.data());
                        std::cout << "size is: " << m_size << endl;
                        do_read_body();


                    //do_exec_sql(data,size);
                }
                else
                {
                    do_read();
                }

            });
    }

private:
    void do_read_body()
    {
        auto self(shared_from_this());
        async_read(m_socket, m_RBuf, transfer_exactly(m_size) ,
            [this, self] (const err_code& ec, size_t len)
            {
                do_exec_sql(len);
            });

    }


    void do_exec_sql (int len)
    {
        try
        {
            Driver* driver = mysql::get_driver_instance();
            shared_ptr<Connection> conn(driver->connect("tcp://127.0.0.1:3306", "root", "321"));
            conn->setSchema("student");


            string execSql;
            boost::archive::binary_iarchive ia(m_RBuf);
            ia & execSql;

            std::cout << "send len is : " << len << endl;
            std::cout << "sql is : " << execSql << endl;

            int param = 0;
            ia & param;
            std::cout << "parm1: " << param << endl;

            ia & param;
            std::cout << "parm2: " << param << endl;



            std::shared_ptr<sql::Statement> stmt(conn->createStatement());
            std::shared_ptr<ResultSet>      res(stmt->executeQuery(execSql));



            boost::archive::binary_oarchive oa(m_WBuf);
            while (res->next())
            {
                int id = res->getInt("id");
                std::cout << "id: " << id << std::endl;
                oa & id;
                //m_strData += std::to_string(id);
            }

            do_write();

        }
        catch (std::exception& ex)
        {
            cout << "sql error: " <<ex.what() << endl;
        }


    }

private:
    void do_write ()
    {
        cout << "buf size: " << m_WBuf.size() << endl;
        auto self = shared_from_this();
        async_write(m_socket, m_WBuf,
          transfer_exactly(m_WBuf.size()),
          [this,self] (const err_code& ec,std::size_t bytes_transferred)
          {
              if (!ec)
              {
                  do_read();
              }
              else
              {
                   m_socket.close();
              }
          });
    }


private:
    ip::tcp::socket m_socket;
    std::array<char,4> data ;
    std::vector<int>    m_vecData;
    std::string m_strData;

    boost::asio::streambuf m_WBuf;
    boost::asio::streambuf m_RBuf;

    int m_size;
};




class Server
{
public:
    using err_code = boost::system::error_code;


public:
   Server(io_service& io, unsigned short port)
     :m_socket(io),
      m_acceptor(io, ip::tcp::endpoint(ip::tcp::v4(), port))
   {
       do_accept();
   }

private:
    void do_accept ()
    {
        m_acceptor.async_accept(m_socket,
          [this] (const err_code& ec)
          {
              if (!ec)
              {
                  cout << m_socket.remote_endpoint().address().to_string() << endl;
                  std::make_shared<Session>(std::move(m_socket))->start();
              }
              do_accept();
          });
    }
private:
    ip::tcp::socket   m_socket;
    ip::tcp::acceptor m_acceptor;

};


int main()
{
    io_service io;
    Server s(io, 9800);
    io.run();
}
