package wuxi99
import javax.mail.Message
import javax.mail.Session
import javax.mail.Transport
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import java.util.Date
import java.util.Properties
import grizzled.slf4j.Logger
trait Email {
	def sendMail(mailTitle:String,mailContent:String)
	def sendMailFL(mailTitle:String,fileName:String,line:Int,AppCount:Int){
		val mailContent = s"""<b>文件:${fileName}</b><br>
			<b>行数:${line}</b><br>
			<b>应用数:${AppCount}</b>"""
		sendMail(mailTitle,mailContent)
	}
 def sendMailHtml(mailTitle:String,mailContent:String){
    val mailContentHtml = s"<b>${mailContent}</b>"
    sendMail(mailTitle,mailContentHtml)
  }
}

object MailUtility extends Email {
	private val smtpServer = "smtp.exmail.qq.com"
	private val fromMail = "service@adups.com"
	private val toMail = "yangshuxuan@adups.com"
	private val user = fromMail
	private val password = "ys123456"
	@transient lazy val logger = Logger[this.type]


	def sendMail(mailTitle:String,mailContent:String) {
	  try {	
		val props = new Properties()
		props.put("mail.smtp.host",smtpServer )
		props.put("mail.smtp.auth", "true")
		val session = Session.getInstance(props)

		val message = new MimeMessage(session)
		message.setFrom(new InternetAddress(fromMail))
		message.setRecipient(Message.RecipientType.TO, new InternetAddress(toMail))
		message.setSubject(mailTitle)
		message.setContent(mailContent, "text/html;charset=utf-8")
		//message.setText(mailContent)
		message.setSentDate(new Date())
		message.saveChanges()

		val transport = session.getTransport("smtp")
		transport.connect(user, password)
		transport.sendMessage(message, message.getAllRecipients())
		transport.close()
	  } catch {
		case _ : Throwable => logger.info("This Mail Didnot Send:[ " + mailTitle + " ] [ " + mailContent + "]" )
          }	
	}

}
