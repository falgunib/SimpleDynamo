package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	public static String pred, succ , curr;
	public static String myPort, predHash, succHash;
	static final int SERVER_PORT = 10000;
	String portStr, port;
	static final HashMap<String,String> nodes = new HashMap<String,String>();
	private ReentrantLock insert_lock = new ReentrantLock();
    private ReentrantLock query_lock = new ReentrantLock();


	final Uri myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	public static final String TABLE_NAME = "Msgs";
	public static final String DATABASE_NAME = "SimpleDht.db";
	public static final String KEY = "key";
	public static final String VALUE = "value";
	public static final int DATABASE_VERSION = 1;
	public static int z=0;
	public class MyDBHelper extends SQLiteOpenHelper {

		public MyDBHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}

		public void onCreate(SQLiteDatabase db) {
			//CREATE TABLE t(x TEXT PRIMARY KEY, y TEXT);
			db.execSQL("CREATE TABLE " + TABLE_NAME + "( " + KEY + " TEXT PRIMARY KEY, " + VALUE + " TEXT);");
		}

		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			Log.v("Upgrading", TABLE_NAME);
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
			onCreate(db);
		}
	}
	private MyDBHelper dbHelp;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		SQLiteDatabase db = dbHelp.getWritableDatabase();
		if(selection.equals("*") || selection.equals("@")){
			db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME + ";");
		}
		db.delete(TABLE_NAME,"( key = '"+selection+"' )",selectionArgs);
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
        Log.v("Insert Locking",portStr);
        //insert_lock.lock();
		try {
			String key1 = values.getAsString("key");
			String value = values.getAsString("value");
            String key=null;
            Log.v("For key",key1);
            dbHelp = new MyDBHelper(getContext());
            SQLiteDatabase db = dbHelp.getWritableDatabase();
			String[] keys = key1.split("!!");
			Log.v("Keys are ",key1);
            if(keys.length>1) {
                key = keys[1];
                values.put("key",key);

				Log.v("1 Insert","Keys > 1 ");
                if (keys[0].equals("Replicate")) {
                    Log.d("We are replicating!",values.getAsString("key"));
                    db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    Log.v("Insert:", values.toString());
                    Log.v("Insert Unlocking",portStr);
                    //insert_lock.unlock();
					return null;

                }
				else if(keys[0].equals("New")){
					//add & then send to 2 succ
					Log.d("2 New key, passing on",values.getAsString("key"));
					db.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succ)*2);

						String m = succ + "&&" + "Replicate"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
						Log.v("Send 1 for insert", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						Log.v("Got back this "," " + s);
						socket.close();


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception e) {
						e.printStackTrace();
					}
					String succ2 = nodes.get(genHash(succ));
					Log.v("Port: "+portStr+" Succ: "+succ+" Succ2",succ2);
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(succ2)*2);

						String m = succ2 + "&&" + "Replicate"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
						Log.v("Send 2 for insert", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						Log.v("Got back this "," " + s);
						socket.close();


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch (Exception e) {
						e.printStackTrace();
					}
					return null;
				}
				//return null;
            }

            else
                key = keys[0];
            values.put("key",key);
            String hashOfKey = genHash(key);
			Log.v("Coming to hashOfKey",hashOfKey);
			//if(hashOfKey.charAt(0) == '0')
			//	hashOfKey = hashOfKey.substring(1);
			myPort = genHash(portStr);

			if(pred!=null)
				predHash = genHash(pred);
			if(succ!=null)
				succHash = genHash(succ);
			if(hashOfKey.compareTo(genHash("5562")) >= 0 && hashOfKey.compareTo(genHash("5556")) < 0){
				//between A & B send to B
				Log.v("3 Insert","Between 5562 & 5556");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5562",genHash("5562"));
				Log.v("Hash of 5556",genHash("5556"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5556")*2);

					String m = "5556" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5556", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					Log.v("Got back this "," " + s);
					socket.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}


			}
			else if(hashOfKey.compareTo(genHash("5556")) >= 0 && hashOfKey.compareTo(genHash("5554")) < 0){
				//between B & C send to C
				Log.v("4 Insert","Between 5556 & 5554");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5556",genHash("5556"));
				Log.v("Hash of 5554",genHash("5554"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5554")*2);

					String m = "5554" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5554", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					Log.v("Got back this "," " + s);
					socket.close();

				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			else if(hashOfKey.compareTo(genHash("5554")) >= 0 && hashOfKey.compareTo(genHash("5558")) < 0){
				//between C & D send to D
				Log.v("5 Insert","Between 5554 & 5558");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5554",genHash("5554"));
				Log.v("Hash of 5558",genHash("5558"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5558")*2);

					String m = "5558" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5558", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					Log.v("Got back this "," " + s);
					socket.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			else if(hashOfKey.compareTo(genHash("5558")) >= 0 && hashOfKey.compareTo(genHash("5560")) < 0){
				//between D & E send to E
				Log.v("6 Insert","Between 5558 & 5560");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5558",genHash("5558"));
				Log.v("Hash of 5560",genHash("5560"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5560")*2);

					String m = "5560" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5560", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					Log.v("Got back this "," " + s);
					socket.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			else if(hashOfKey.compareTo(genHash("5562")) < 0 ){
				//min less than A send to A
				Log.v("7 Insert","Less than 5562");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5562",genHash("5562"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5562")*2);

					String m = "5562" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5562", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();

					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					Log.v("Got back this "," " + s);
					socket.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			else if(hashOfKey.compareTo(genHash("5560")) >= 0 ){
				//max greater than E send to A
				Log.v("8 Insert","Greater than 5560");
				Log.v("Hash of key",hashOfKey);
				Log.v("Hash of 5560",genHash("5560"));
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("5562")*2);

					String m = "5562" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
					Log.v("Sending key to 5562", m);
					PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
					pw.println(m + "\r\n");
					pw.flush();

					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String s = br.readLine();
					if(s==null){


						String m1 = "5562" + "&&" + "New"+"!!"+key + ",," + value + "&&" + "Insert_New_Key";
						Log.v("Sending key to 5562", m1);
						pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m1 + "\r\n");
						pw.flush();
					}
					Log.v("Got back this "," " + s);
					socket.close();


				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}


		}catch (Exception e){
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		try {
			TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
			portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);//5554
			port = String.valueOf((Integer.parseInt(portStr) * 2));//11108
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch(NullPointerException e1){
			e1.printStackTrace();
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}
        try {
            nodes.put(genHash("5554"), "5558");
            nodes.put(genHash("5556"), "5554");
            nodes.put(genHash("5558"), "5560");
            nodes.put(genHash("5560"), "5562");
            nodes.put(genHash("5562"), "5556");
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join", port );

		dbHelp = new MyDBHelper(getContext());
		SQLiteDatabase db = dbHelp.getWritableDatabase();
		return false;
	}
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(msgs[1]));
				//Log.v("msgs[0] is",msgs[0]);
				String m = msgs[1] + "&&" + portStr + "&&" + msgs[0];
				//Log.v("Sending msg from client task", m);
				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				pw.println(m + "\r\n");
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (Exception e) {
				e.printStackTrace();
			}

			return null;
		}
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			//Log.v("Total nodes active"," ");
			if(serverSocket != null) {
				try {
					while (true) {//m:toport + from port + join + portnumber(myport?)
						//Log.v("In server of "+portStr, genHash(portStr));
                        //System.out.println("Nodes are: "+nodes);
						Socket clientSocket = serverSocket.accept();
						BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
						String s = br.readLine();
						Log.v("Accepted",portStr);
						s = s.trim();
						String parts[] = s.split("&&");//splitting the port & msg from s
						if(parts[2].equals("Join")) {
                            if (parts[1].equals("5554")) {
                                pred = "5556";
                                succ = "5558";
                                curr = parts[1];
                                Log.e("My port : ",parts[1]);
                                Log.e("My pred : ", pred);
                                Log.e("My succ : ", succ);

                            }
                            else if (parts[1].equals("5556")) {
                                pred = "5562";
                                succ = "5554";
                                curr = parts[1];
                                Log.e("My port : ",parts[1]);
                                Log.e("My pred : ", pred);
                                Log.e("My succ : ", succ);

                            }
                            else if (parts[1].equals("5558")) {
                                pred = "5554";
                                succ = "5560";
                                curr = parts[1];
                                Log.e("My port : ",parts[1]);
                                Log.e("My pred : ", pred);
                                Log.e("My succ : ", succ);

                            }
                            else if (parts[1].equals("5560")) {
                                pred = "5558";
                                succ = "5562";
                                curr = parts[1];
                                Log.e("My port : ",parts[1]);
                                Log.e("My pred : ", pred);
                                Log.e("My succ : ", succ);

                            }
                            else if (parts[1].equals("5562")) {
                                pred = "5560";
                                succ = "5556";
                                curr = parts[1];
                                Log.e("My port : ",parts[1]);
                                Log.e("My pred : ", pred);
                                Log.e("My succ : ", succ);

                            }

						} else if (parts[2].equals("Update_Pred")) {
							//Log.v("In client server task pred", portStr);
							pred = parts[1];
							curr = parts[0];
							//succ = parts[1];
							// Log.v("Pred updated to", pred);

						} else if (parts[2].equals("Update_Succ")) {
							//Log.v("In client server task succ", portStr);
							succ = parts[1];
							curr = parts[0];
							//Log.v("Succ updated to", succ);

						} else if(parts[2].equals("Insert_New_Key")) {
							ContentValues cv = new ContentValues();
							String keys[] = parts[1].split(",,");
							Log.v("!!MAIN!!keys "+keys[0],keys[1]);
                            keys[0] = keys[0];
							cv.put("key",keys[0]);
							cv.put("value", keys[1]);
							insert(null, cv);
							String m = "Received inserted Main";
							PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
							pw.println(m + "\r\n");
							pw.flush();
						}
						else if(parts[2].equals("Query_Key")) {

							SQLiteDatabase db = dbHelp.getReadableDatabase();
							Log.v("In main query"," ");
							Cursor c = null;
							c = query(myUri, null, parts[1], null, null, null);
							//Log.v("Lets check cursor"," ");
							//Log.v("Returning m", Integer.toString(c.getCount()));
							c.moveToFirst();
							String qkey = c.getString(c.getColumnIndex("key"));
							String qvalue = c.getString(c.getColumnIndex("value"));
							Log.v(qkey,qvalue);
							String m = pred + "&&" + qkey + ",," + qvalue + "&&" + "Query_Key";
							//Log.v("Returning m", m);
							//Log.v("Sending update pred msg to client", m);
							PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
							pw.println(m + "\r\n");
							//Log.v("Returning query", " ");

						}
						else if(parts[2].equals("Query_All")){
							//Log.v("Query all"," ");
							Log.v(parts[0],pred);
							Log.v("z",Integer.toString(z));
							if(z!=1) {
								Log.v("Msg I got",s);
								Cursor c = query(myUri, null, parts[1], null, null, null);
								Log.v("Lets check cursor", " ");
								String qkey, qvalue;
								Log.v("Returning m", Integer.toString(c.getCount()));
								c.moveToFirst();
								ArrayList all = new ArrayList<String>();
								while (!c.isAfterLast()) {
									Log.v("Inside","while");
									qkey = c.getString(c.getColumnIndex("key"));
									qvalue = c.getString(c.getColumnIndex("value"));
									Log.v(qkey, qvalue);
									//Log.v("Sending update pred msg to client", m);

									String m = qkey + "&&" + qvalue;
									Log.v("Returning m", m);

									all.add(m);
									//Log.v("Returning query", " ");
									c.moveToNext();
								}
								String n = all.toString();
								Log.v("n", n);
								PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), false);
								pw.println(n + "\r\n");
								pw.flush();
								z=0;
							}
							else {
								Log.v("Coming back to ",parts[0]);
								String m =null;
								PrintWriter pw = new PrintWriter(clientSocket.getOutputStream(), true);
								pw.println(m + "\r\n");
								//return null;
							}
						}

						//Log.v("Pred of this", pred);
						//Log.v("Succ of this", succ);
						//clientSocket.close();
					}

				} catch (Exception e) {
					Log.e(TAG, "Error in server passing messages "+e);
				}
			}

			return null;
		}

		protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */


			return;
		}
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
        //Log.v("First Query Locking",selection);

		try {
			z=0;
			Log.v("Query Blah"," ");
			Cursor c1;
			String hashOfKey = genHash(selection);
			//if(hashOfKey.charAt(0) == '0')
			//	hashOfKey = hashOfKey.substring(1);
			MatrixCursor c = new MatrixCursor(new String[]{KEY,VALUE});

			if(succ!=null)
				succHash = genHash(succ);
			if(pred!=null)
				predHash = genHash(pred);

			myPort = genHash(portStr);
			SQLiteDatabase db = dbHelp.getReadableDatabase();

			if ((selection).equals("@")) {
				Cursor cursor = db.rawQuery("SELECT * FROM " + TABLE_NAME, null);
				Log.v("Query:", selection);
                //Log.v("Query Unlocking",selection);
                //query_lock.unlock();
				return cursor;
			}
			else if ((selection).equals("*")) {
				Cursor cursor = db.rawQuery("SELECT * FROM " + TABLE_NAME, null);
				Log.v("1 Cursor count of "+portStr,Integer.toString(cursor.getCount()));
				//Log.v("succ",succ);
				if( succ!=null && pred!=null) {
					if (!(succ == curr)) {


						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(succ) * 2);
							z++;
							String m = portStr + "&&" + selection + "&&" + "Query_All" ;
							Log.v("1 Sending key to succ *", m);
							PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
							pw.println(m + "\r\n");
							//read
                            //Log.v("Query Locking",selection);
                            //query_lock.lock();
							BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							String s = br.readLine();
							Log.v("stuff b4 null"," ");

							if(s.equals("null") || s.equals("[]")) {

								Log.v("1 Cursor count of "+portStr,Integer.toString(cursor.getCount()));
								//query_lock.unlock();
								return cursor;
							}
							Log.v("s", s);
							s=s.substring(1, s.length() - 1);
							Log.v("s", s);
							String[] parts = s.split(", ");

							for( int i=0;i< parts.length;i++) {
								String[] keys = parts[i].split("&&");
								Log.v("Received key", keys[0]);
								c.addRow(new Object[]{keys[0], keys[1]});
							}
							socket.close();
							MergeCursor mC = new MergeCursor(new Cursor[]{cursor, c});
							Log.v("1 mCount",Integer.toString(mC.getCount()));
							z=0;
                            //Log.v("Query Unlocking",selection);
                            //query_lock.unlock();
							return mC;

						} catch (UnknownHostException e) {
							Log.e(TAG, "ClientTask UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "ClientTask socket IOException");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}
                //query_lock.unlock();
				return cursor;
			}
			if(pred == curr){
                //query_lock.lock();
				Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
				//Log.v("!!!Query:", selection);
                Log.v("2 Query Unlocking",selection);
                //query_lock.unlock();
				return cursor;
			}
			else if(predHash!=null && succHash!=null) {
				//Log.v("3 pred & succ not null"," ");
				Log.v("3 Blah 1", selection);
				Log.v("3 Hash of key",genHash(selection));
				Log.v("3 Hash of succ",succHash);
				Log.v("3 Hash of pred",predHash);
				if(hashOfKey.compareTo(genHash(portStr))<= 0 && hashOfKey.compareTo(predHash) > 0){
					Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
					Log.v("Me 1!!!Query:", selection);
					//Log.v("2 Query Unlocking",selection);
					//query_lock.unlock();
					return cursor;
				}
				else if (hashOfKey.compareTo(genHash(portStr)) > 0 && hashOfKey.compareTo(predHash) > 0 && predHash.compareTo(genHash(portStr)) > 0) {
					Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
					Log.v("Me 2!!!Query:", selection);
					//Log.v("2 Query Unlocking",selection);
					//query_lock.unlock();
					return cursor;
				}
				else if (hashOfKey.compareTo(genHash(portStr)) < 0 && hashOfKey.compareTo(succHash) < 0 && predHash.compareTo(genHash(portStr)) > 0) {
					Cursor cursor = db.query(TABLE_NAME, projection, "( key = '" + selection + "' )", selectionArgs, null, null, null);
					Log.v("Me 3!!!Query:", selection);
					//Log.v("2 Query Unlocking",selection);
					//query_lock.unlock();
					return cursor;
				}
				if(hashOfKey.compareTo(genHash("5562")) >= 0 && hashOfKey.compareTo(genHash("5556")) < 0){
					//between A & B send to D
					Log.v("3 Query","Between 5562 & 5556");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5562",genHash("5562"));
					Log.v("Hash of 5556",genHash("5556"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5556")*2);

						String m = "5556" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5558", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");
						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch(Exception e){
						e.printStackTrace();
					}


				}
				else if(hashOfKey.compareTo(genHash("5556")) >= 0 && hashOfKey.compareTo(genHash("5554")) < 0){
					//between B & C send to E
					Log.v("4 Query","Between 5556 & 5554");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5556",genHash("5556"));
					Log.v("Hash of 5554",genHash("5554"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5554")*2);

						String m = "5554" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5560", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");
						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;

					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch(Exception e){
						e.printStackTrace();
					}

				}
				else if(hashOfKey.compareTo(genHash("5554")) >= 0 && hashOfKey.compareTo(genHash("5558")) < 0){
					//between C & D send to A
					Log.v("5 Query","Between 5554 & 5558");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5554",genHash("5554"));
					Log.v("Hash of 5558",genHash("5558"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5558")*2);

						String m = "5558" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5562", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");
						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch(Exception e){
						e.printStackTrace();
					}

				}
				else if(hashOfKey.compareTo(genHash("5558")) >= 0 && hashOfKey.compareTo(genHash("5560")) < 0){
					//between D & E send to B
					Log.v("6 Query","Between 5558 & 5560");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5558",genHash("5558"));
					Log.v("Hash of 5560",genHash("5560"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5560")*2);

						String m = "5560" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5556", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");
						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch(Exception e){
						e.printStackTrace();
					}

				}
				else if(hashOfKey.compareTo(genHash("5562")) <= 0 ){
					//min less than A send to C
					Log.v("7 Query","Less than 5562");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5562",genHash("5562"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5562")*2);

						String m = "5562" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5554", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");
						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					} catch(Exception e){
						e.printStackTrace();
					}

				}
				else if(hashOfKey.compareTo(genHash("5560")) >= 0 ){
					//max greater than E send to C
					Log.v("8 Query","Greater than 5560");
					Log.v("Hash of key",hashOfKey);
					Log.v("Hash of 5560",genHash("5560"));
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt("5562")*2);

						String m = "5562" + "&&" + selection + "&&" + "Query_Key";
						Log.v("Sending key to 5554", m);
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(m + "\r\n");
						pw.flush();
						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						String s = br.readLine();
						String parts[] = s.split("&&");

						String keys[] = parts[1].split(",,");
						Log.v("4 Received key back", s);
						c.addRow(new Object[]{keys[0], keys[1]});
						c1 = c;
						//Log.v("4 Query Unlocking",selection);
						//query_lock.unlock();
						socket.close();
						//Log.v("Query Unlocking",selection);
						//query_lock.unlock();
						return c1;


					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException");
					}
					catch(Exception e){
						e.printStackTrace();
					}

				}
			}

		}catch(Exception e){
			e.printStackTrace();
		}

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
}
