package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String myPortNumber ;
    int predecessor= -1,successor = -1,noOfEntries = 0;
    static final int SERVER_PORT = 10000;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        // Find Self port
        Log.e(TAG,"Inside OnCreate()");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPortNumber = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.e(TAG,"My Port Number"+myPortNumber);
        //create server sockets to continuously listen from other nodes
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
        }

        // Join Ring
        if(myPortNumber.equals("11108"))
        {
            predecessor = -1;
            successor= -1;
            Log.e(TAG, "I am "+myPortNumber+ " My Predecessor = " +predecessor +" Successor = " + successor);
        }
        else
        {
            //find correct position
            String msg = "0" + myPortNumber;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);
            Log.e(TAG, "I am "+myPortNumber+ " My Predecessor = " +predecessor +" Successor = " + successor);

        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            int seq_no = 0;
            try {
                while (true) {
                    Socket client = null;

                    client = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String msg;
                    PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
                   // pw.println(msgToSend);
                    // Log.e(TAG, "Before Reading");
                    msg = br.readLine();
                    String reply;
                    if(msg.charAt(0) == '0')
                    {
                        //join msg
                        Log.e(TAG, "Serving Join Request");
                        int id = Integer.parseInt(msg.substring(1,6));

                        if(id <= Integer.parseInt(myPortNumber))
                        {
                            Log.e(TAG, "Found Position");
                        int oldpred = predecessor;
                        predecessor = Integer.parseInt(id);
                        reply = oldpred + myPortNumber;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    oldpred);
                        String msgToSend = "4" + id;
                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        pw.println(msgToSend);
                        }
                        else
                        {

                            Log.e(TAG, "Still not found position..Forwarding Req to Successor");
                            int server ;
                            if(successor == -1)
                            {
                                //2 nd node is getting added;
                                successor = id;
                                predecessor = id;
                                reply = myPortNumber + myPortNumber;
                            }
                            else
                            {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        successor);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                                String msgToSend = msg;
                                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                pw.println(msgToSend);
                                reply = in.readLine();
                            }

                        }
                        pwMain.write(reply);
                    }
                    else if(msg.charAt(0) == '1')
                    {
                        //insert msg
                    }
                    else if(msg.charAt(0) == '2')
                    {
                        //query msg
                    }
                    else if(msg.charAt(0) == '3')
                    {
                    //delete msg
                    }
                    else if(msg.charAt(0) == '4')
                    {
                        //Successor Change
                        successor = Integer.parseInt(msg.substring(1,6));
                        Log.e(TAG, "I am "+myPortNumber+ " My Predecessor = " +predecessor +" Successor = " + successor);
                    }
                    else
                    Log.e(TAG,"Wrong Message");


                }
            }catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    11108);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String inputLine;
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            pw.println(msgs[0]);
            String reply = in.readLine();
            predecessor = Integer.parseInt(reply.substring(0,5));
            successor = Integer.parseInt(reply.substring(5,10));
            Log.e(TAG, "I am "+myPortNumber+ " My Predecessor = " +predecessor +" Successor = " + successor);

        } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
      //      sendActualMsg(msgs[0]);
            return null;
        }
    }

}


