package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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
    int myPort, predecessorPort = -1,successorPort = -1;
    String myPortHash, predecessorPortHash, successorPortHash;
    static final int SERVER_PORT = 10000;
    static final int MAIN_AVD_PORT = 11108;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        // TODO Handle cases when there are only single node or 2 nodes
        String key,value;
        Log.e(TAG, "Inside Content Provider Insert");
        handleDeleteMsg(selection);
        return 0;
    }

    private void handleDeleteMsg(String selection) {
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        // TODO Handle cases when there are only single node or 2 nodes
        return null;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        // Insert Request from Tester or Grader will be received here
        // TODO Handle cases when there are only single node or 2 nodes
        String key,value;
        Log.e(TAG, "Inside Content Provider Insert");
        key = (String)values.get("key");
        value = (String)values.get("value");
        Log.e(TAG, "Received Key = " + key + " Value=" + value);
        handleInsertMsg(key,value);

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        // Find Self port
        Log.e(TAG,"Inside OnCreate()");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = (Integer.parseInt(portStr) * 2);
        try {
            myPortHash = genHash(String.valueOf(myPort));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.e(TAG,"My Port Number"+ myPort);
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
        if(myPort == MAIN_AVD_PORT)
        {
            predecessorPort = -1;
            successorPort= -1;
            Log.e(TAG, "I am "+ myPort +  "With Hash Value =" + myPortHash );
        }
        else
        {
            //find correct position
            String msg = "0" + myPort;
            Log.e(TAG, "My Port is " + myPort + "With Hash Value =" + myPortHash + "Sending Join Request to Avd0");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msg);

        }

        return true;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    boolean isBelongTO(String idHash)
    {
        Log.e(TAG,"New Node Hash = " + idHash + " My Hash =" + myPortHash+ " predcessorHash =" + predecessorPortHash + " SuccessorHash =" + successorPortHash);
        if((idHash.compareTo(predecessorPortHash) > 0 && idHash.compareTo(myPortHash) < 1)
                || (((predecessorPortHash.compareTo(myPortHash))) > 0 ) && (idHash.compareTo(predecessorPortHash) >0 || idHash.compareTo(myPortHash) < 1))
            return true;
        else return false;
    }

    private void handleInsertMsg(String key, String value) {
        if(isBelongTO(key))
        {
            FileOutputStream outputStream;

            try {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                Log.v("GroupMessengerActivity", "Opened File");
                outputStream.write(value.getBytes());
                Log.v("GroupMessengerActivity", "Written on File");
                outputStream.close();
            } catch (Exception e) {
                Log.v("GroupMessengerActivity", "Exception in Writing onto Disk");
            }
            Log.v("GroupMessengerActivity", "Insertion Successful");

        }
        else
        {
            Log.e(TAG, "Still not found position..Forwarding Req to Successor");
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        successorPort);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter pw = null;
                pw = new PrintWriter(socket.getOutputStream(), true);
                String msg = "1" + key + value;
                pw.println(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
                    Log.e(TAG, "Server Accepted Request");
                    BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String msg;

                   // pw.println(msgToSend);
                    // Log.e(TAG, "Before Reading");
                    msg = br.readLine();
                    String reply;
                    if(msg.charAt(0) == '0')
                    {
                        //join msg
                        Log.e(TAG, "Serving Join Request");
                        int id = Integer.parseInt(msg.substring(1,6));
                        String idHash = genHash(id+"");
                        if(successorPort == -1)
                        {
                            //2 nd node is getting added;
                            successorPort = id;
                            successorPortHash = genHash(String.valueOf(successorPort));
                            predecessorPort = id;
                            predecessorPortHash = genHash(String.valueOf(predecessorPort));
                            reply = String.valueOf(myPort) + myPort;
                            Log.e(TAG, "Sending reply" + reply);
                        }
                        else if(isBelongTO(idHash))
                        {
                            Log.e(TAG, "Found Position");
                        int oldpred = predecessorPort;
                        predecessorPort = id;
                        reply = oldpred + String.valueOf(myPort);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    oldpred);
                        String msgToSend = "4" + id;
                        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                        pw.println(msgToSend);
                        }
                        else
                        {

                            Log.e(TAG, "Still not found position..Forwarding Req to Successor" + successorPort);

                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        successorPort);
                                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
                                pw.println(msg);


                            reply = in.readLine();
                            Log.e(TAG, "Received Reply from " + successorPort + " "+  reply);
                        }
                        Log.e(TAG, "Sending reply" + reply);
                        PrintWriter pwMain = new PrintWriter(client.getOutputStream(), true);
                        pwMain.println(reply);
                    }
                    else if(msg.charAt(0) == '1')
                    {
                        String key="",value = "";
                        handleInsertMsg(key,value);
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
                        successorPort = Integer.parseInt(msg.substring(1,6));
                        successorPortHash = genHash(String.valueOf(successorPort));
                        Log.e(TAG, "I am "+ myPort + " My Predecessor = " + predecessorPort +" Successor = " + successorPort);
                    }
                    else
                    Log.e(TAG,"Wrong Message");

                }
            }catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
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
                    MAIN_AVD_PORT);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String inputLine;
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            pw.println(msgs[0]);
            inputLine= in.readLine();
            Log.e(TAG,"Received Reply :" + inputLine);
            predecessorPort = Integer.parseInt(inputLine.substring(0,5));
            predecessorPortHash = genHash(String.valueOf(predecessorPort));
            successorPort = Integer.parseInt(inputLine.substring(5,10));
            successorPortHash = genHash(String.valueOf(successorPort));
            Log.e(TAG, "I am "+ myPort + " My Predecessor = " + predecessorPort +" Successor = " + successorPort);

        } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            //      sendActualMsg(msgs[0]);
            return null;
        }
    }

}


