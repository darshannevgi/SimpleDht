package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.e(TAG,"Verifying LDUMP output");
                new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "@");
            }
        });
        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Log.e(TAG,"Verifying GDUMP output");
                new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "*");

            }
        });
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    private class Task extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {
            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            Cursor resultCursor = getContentResolver().query(mUri, null, params[0], null, null);
            resultCursor.moveToFirst();
           // Log.e(TAG, "Verifying GDUMP output");
            do {
                Log.e(TAG, "Key: " + resultCursor.getString(resultCursor.getColumnIndex("key")) + " Value :" + resultCursor.getString(resultCursor.getColumnIndex("value")));
            }while (resultCursor.moveToNext());
            return null;
        }
    }

}
