package cn.oxygenbar.constants;
/**
 * 程序的返回值
 * 
 * @author 阳
 * 
 */
public class MessageCode {
	// 数据库相关的
	/**
	 * 日志是否打印sql语句
	 */
	public static final boolean SHOW_SQL = true; // 日志是否打印sql语句
	// 系统默认常量(成功)200+
	/**
	 * 正常返回
	 */
	public static final int DEFAULT_NOMAL = 200;
	// 系统默认常量(请求失败)400+
	/**
	 * 请求错误
	 */
	public static final int DEFAULT_REQUEST = 400;
	// 系统默认常量(内部失败)500+
	/**
	 * 服务器内部错误
	 */
	public static final int DEFAULT_WEB = 500;
	// 用户相关的(登陆注册修改密码找回密码等等)相关的300+
	/**
	 * 用户操作失败
	 */
	public static final int USER_FAIL = 300;
	// 图片上传相关的600+
	/**
	 * 图片上传失败
	 */
	public static final int IMAGE_UPLOAD_FAIL = 600;
	// 短信通知相关的700+
	/**
	 * 短信发送失败
	 */
	public static final int MSGCODE_SEND_FAIL = 700;
	// 邮件发送相关的800+
	/**
	 * 邮件发送失败
	 */
	public static final int EMIL_SEND_FAIL = 800;

}